"""
Enhanced SQL lineage utilities with CTE expansion and alias resolution.

This module provides advanced transformation logic extraction that:
- Expands CTE column references to their actual calculations
- Replaces table aliases with full table URNs
- Recursively processes nested CTEs
- Provides readable transformation logic even for complex queries
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Union

import sqlglot
import sqlglot.expressions as exp

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.errors import SdkUsageError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import DatasetUrn, QueryUrn, SchemaFieldUrn
from datahub.sdk._utils import DEFAULT_ACTOR_URN
from datahub.sdk.main_client import DataHubClient
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.sql_parsing.fingerprint_utils import generate_hash
from datahub.sql_parsing.sqlglot_lineage import get_dialect
from datahub.utilities.ordered_set import OrderedSet

if TYPE_CHECKING:
    from datahub.sql_parsing.sqlglot_lineage import SqlParsingResult

logger = logging.getLogger(__name__)

_empty_audit_stamp = models.AuditStampClass(
    time=0,
    actor=DEFAULT_ACTOR_URN,
)


class CTEDefinition:
    """Represents a CTE (Common Table Expression) definition."""

    def __init__(self, name: str, select_expression: str, column_mappings: Dict[str, str]):
        self.name = name
        self.select_expression = select_expression
        self.column_mappings = column_mappings  # output_col -> calculation


def extract_cte_definitions(
    sql: str,
    platform: str,
    override_dialect: Optional[str] = None,
) -> Dict[str, CTEDefinition]:
    """
    Extract CTE definitions from SQL query.

    Returns a mapping of CTE name -> CTEDefinition with column calculations.
    """
    dialect = get_dialect(override_dialect or platform)

    try:
        statement = sqlglot.parse_one(sql, dialect=dialect)
    except Exception as e:
        logger.debug(f"Failed to parse SQL for CTE extraction: {e}")
        return {}

    ctes = {}

    # Find all CTEs in the query
    for cte in statement.find_all(exp.CTE):
        cte_name = cte.alias
        if not cte_name:
            continue

        # Get the SELECT expression from the CTE
        cte_query = cte.this
        if not isinstance(cte_query, (exp.Select, exp.Union)):
            continue

        # Extract column mappings from the CTE SELECT
        column_mappings = {}

        if isinstance(cte_query, exp.Select):
            for select_col in cte_query.expressions:
                col_name = select_col.alias_or_name
                if col_name and col_name != "*":
                    # Get the SQL expression for this column
                    col_expr = select_col.this if isinstance(select_col, exp.Alias) else select_col
                    column_mappings[col_name] = col_expr.sql(dialect=dialect)

        ctes[cte_name] = CTEDefinition(
            name=cte_name,
            select_expression=cte_query.sql(dialect=dialect),
            column_mappings=column_mappings,
        )

    return ctes


def expand_cte_references(
    transformation_logic: str,
    cte_definitions: Dict[str, CTEDefinition],
    dialect: sqlglot.Dialect,
) -> str:
    """
    Expand CTE column references in transformation logic to their actual calculations.

    Example:
        Input:  COALESCE("pha"."avg_price_30d", "p"."base_price")
        Output: COALESCE(AVG("ph"."new_price"), "p"."base_price")
    """
    if not cte_definitions:
        return transformation_logic

    # Parse the transformation logic
    try:
        expr = sqlglot.parse_one(f"SELECT {transformation_logic}", dialect=dialect)
    except Exception as e:
        logger.debug(f"Failed to parse transformation logic for CTE expansion: {e}")
        return transformation_logic

    # Track if we made any changes
    changed = False

    # Find all column references in the expression
    for col_ref in expr.find_all(exp.Column):
        table_alias = col_ref.table
        col_name = col_ref.name

        # Check if this references a CTE
        if table_alias in cte_definitions:
            cte_def = cte_definitions[table_alias]

            # Check if we have the column definition
            if col_name in cte_def.column_mappings:
                # Replace the column reference with the actual calculation
                calculation = cte_def.column_mappings[col_name]

                # Parse the calculation to create a proper expression node
                try:
                    calc_expr = sqlglot.parse_one(f"SELECT {calculation}", dialect=dialect)
                    # Get just the expression part (not the SELECT wrapper)
                    if isinstance(calc_expr, exp.Select) and calc_expr.expressions:
                        replacement_expr = calc_expr.expressions[0]

                        # Replace the column node with the calculation expression
                        col_ref.replace(replacement_expr.copy())
                        changed = True
                except Exception as e:
                    logger.debug(f"Failed to parse CTE calculation for {table_alias}.{col_name}: {e}")

    if changed:
        # Extract just the expression part (remove the SELECT wrapper)
        if isinstance(expr, exp.Select) and expr.expressions:
            result = expr.expressions[0].sql(dialect=dialect)
        else:
            result = expr.sql(dialect=dialect)

        # Clean up the result
        if result.endswith(" AS " + transformation_logic.split()[-1]):
            # Remove trailing AS clause if present
            result = result[: result.rfind(" AS ")]

        return result

    return transformation_logic


def replace_table_aliases_with_urns(
    transformation_logic: str,
    table_alias_to_urn_mapping: Dict[str, str],
    dialect: sqlglot.Dialect,
) -> str:
    """
    Replace table aliases in transformation logic with readable table names from URNs.

    Example:
        Input:  "p"."base_price"
        Mapping: {"p": "urn:li:dataset:(...raw_products,PROD)"}
        Output: raw_products.base_price
    """
    if not table_alias_to_urn_mapping:
        return transformation_logic

    try:
        expr = sqlglot.parse_one(f"SELECT {transformation_logic}", dialect=dialect)
    except Exception:
        return transformation_logic

    # Find all column references
    for col_ref in expr.find_all(exp.Column):
        table_alias = col_ref.table

        if table_alias and table_alias in table_alias_to_urn_mapping:
            urn_str = table_alias_to_urn_mapping[table_alias]

            # Extract table name from URN
            # URN format: urn:li:dataset:(urn:li:dataPlatform:postgres,ecommerce.public.raw_products,PROD)
            try:
                dataset_urn = DatasetUrn.from_string(urn_str)
                # Get just the table name (last part after last dot or the whole name)
                table_name_parts = dataset_urn.name.split(".")
                table_name = table_name_parts[-1] if table_name_parts else dataset_urn.name

                # Replace the table alias with the actual table name
                col_ref.set("table", table_name)
            except Exception:
                # If URN parsing fails, just use the URN string
                pass

    # Extract the expression
    if isinstance(expr, exp.Select) and expr.expressions:
        result = expr.expressions[0].sql(dialect=dialect, pretty=False)
    else:
        result = expr.sql(dialect=dialect, pretty=False)

    # Remove any trailing AS clause
    if " AS " in result:
        result = result[: result.rfind(" AS ")]

    return result


def enhance_transformation_logic(
    transformation_logic: str,
    sql: str,
    platform: str,
    table_alias_to_urn_mapping: Optional[Dict[str, str]] = None,
    override_dialect: Optional[str] = None,
) -> str:
    """
    Enhance transformation logic by expanding CTEs and replacing aliases.

    This function takes the raw transformation logic (which may contain CTE
    references and table aliases) and produces a more readable version.

    Args:
        transformation_logic: The raw transformation logic string
        sql: The original SQL query (to extract CTE definitions)
        platform: The platform (for dialect detection)
        table_alias_to_urn_mapping: Mapping of table aliases to URNs
        override_dialect: Optional dialect override

    Returns:
        Enhanced transformation logic with CTEs expanded and aliases replaced
    """
    dialect = get_dialect(override_dialect or platform)

    # Step 1: Extract CTE definitions
    cte_definitions = extract_cte_definitions(sql, platform, override_dialect)

    # Step 2: Expand CTE references
    if cte_definitions:
        transformation_logic = expand_cte_references(
            transformation_logic, cte_definitions, dialect
        )

    # Step 3: Replace table aliases with readable names
    if table_alias_to_urn_mapping:
        transformation_logic = replace_table_aliases_with_urns(
            transformation_logic, table_alias_to_urn_mapping, dialect
        )

    return transformation_logic


def infer_lineage_from_sql_with_enhanced_transformation_logic(
    *,
    graph: Union[DataHubGraph, DataHubClient],
    query_text: str,
    platform: str,
    platform_instance: Optional[str] = None,
    env: str = "PROD",
    default_db: Optional[str] = None,
    default_schema: Optional[str] = None,
    override_dialect: Optional[str] = None,
    expand_ctes: bool = True,
    replace_aliases: bool = True,
) -> None:
    """
    Add lineage with enhanced transformation logic that expands CTEs and replaces aliases.

    This function extends the basic transformation logic preservation by:
    1. Expanding CTE column references to their actual calculations
    2. Replacing table aliases with readable table names
    3. Providing fully expanded transformation expressions

    Args:
        graph: DataHubGraph or DataHubClient instance
        query_text: SQL query to parse
        platform: Data platform identifier
        platform_instance: Optional platform instance
        env: Environment (default: "PROD")
        default_db: Default database name
        default_schema: Default schema name
        override_dialect: Optional dialect override
        expand_ctes: Whether to expand CTE references (default: True)
        replace_aliases: Whether to replace table aliases with full names (default: True)

    Example:
        ```python
        # Original transformation logic:
        # COALESCE("pha"."avg_price_30d", "p"."base_price")

        # Enhanced transformation logic:
        # COALESCE(AVG(raw_price_history.new_price), raw_products.base_price)
        ```
    """
    from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result

    # Handle both DataHubClient and DataHubGraph
    if isinstance(graph, DataHubClient):
        actual_graph = graph._graph
    else:
        actual_graph = graph

    # Parse the SQL query
    parsed_result: SqlParsingResult = create_lineage_sql_parsed_result(
        query=query_text,
        default_db=default_db,
        default_schema=default_schema,
        platform=platform,
        platform_instance=platform_instance,
        env=env,
        graph=actual_graph,
        override_dialect=override_dialect,
    )

    # Handle parsing errors
    if parsed_result.debug_info.table_error:
        raise SdkUsageError(
            f"Failed to parse SQL query: {parsed_result.debug_info.error}"
        )
    elif parsed_result.debug_info.column_error:
        logger.warning(
            f"Failed to parse column-level lineage from SQL query: {parsed_result.debug_info.error}",
        )

    if not parsed_result.out_tables:
        raise SdkUsageError(
            "No output tables found in the query. Cannot establish lineage."
        )

    downstream_urn = parsed_result.out_tables[0]

    # Create query URN and entity
    query_urn = QueryUrn(generate_hash(query_text)).urn()
    from datahub.sql_parsing.sql_parsing_aggregator import make_query_subjects

    fields_involved = OrderedSet([str(downstream_urn)])
    for upstream_table in parsed_result.in_tables:
        if upstream_table != downstream_urn:
            fields_involved.add(str(upstream_table))

    if parsed_result.column_lineage:
        for col_lineage in parsed_result.column_lineage:
            if col_lineage.downstream and col_lineage.downstream.column:
                downstream_field = SchemaFieldUrn(
                    downstream_urn, col_lineage.downstream.column
                ).urn()
                fields_involved.add(downstream_field)

            for upstream_ref in col_lineage.upstreams:
                if upstream_ref.table and upstream_ref.column:
                    upstream_field = SchemaFieldUrn(
                        upstream_ref.table, upstream_ref.column
                    ).urn()
                    fields_involved.add(upstream_field)

    query_entity = MetadataChangeProposalWrapper.construct_many(
        query_urn,
        aspects=[
            models.QueryPropertiesClass(
                statement=models.QueryStatementClass(
                    value=query_text,
                    language=models.QueryLanguageClass.SQL,
                ),
                source=models.QuerySourceClass.SYSTEM,
                created=_empty_audit_stamp,
                lastModified=_empty_audit_stamp,
            ),
            make_query_subjects(list(fields_involved)),
        ],
    )

    # Build table alias to URN mapping for alias replacement
    table_alias_to_urn: Dict[str, str] = {}
    if replace_aliases:
        # We need to parse the SQL again to get table aliases
        dialect = get_dialect(override_dialect or platform)
        try:
            statement = sqlglot.parse_one(query_text, dialect=dialect)

            # Find all table references and their aliases
            for table_ref in statement.find_all(exp.Table):
                table_alias = table_ref.alias_or_name
                table_name = table_ref.name

                # Try to match this table to one of our URNs
                for urn in parsed_result.in_tables:
                    if table_name.lower() in urn.lower():
                        table_alias_to_urn[table_alias] = urn
                        break
        except Exception as e:
            logger.debug(f"Failed to extract table aliases: {e}")

    # Process each upstream table
    for upstream_table in parsed_result.in_tables:
        if upstream_table == downstream_urn:
            continue

        fine_grained_lineages: List[models.FineGrainedLineageClass] = []

        if parsed_result.column_lineage:
            for col_lineage in parsed_result.column_lineage:
                if not (col_lineage.downstream and col_lineage.downstream.column):
                    continue

                upstream_refs = [
                    ref
                    for ref in col_lineage.upstreams
                    if ref.table == upstream_table and ref.column
                ]

                if not upstream_refs:
                    continue

                # Extract and enhance transformation logic
                transform_operation = None
                if col_lineage.logic:
                    raw_logic = col_lineage.logic.column_logic

                    # Enhance the transformation logic
                    if expand_ctes or replace_aliases:
                        try:
                            enhanced_logic = enhance_transformation_logic(
                                transformation_logic=raw_logic,
                                sql=query_text,
                                platform=platform,
                                table_alias_to_urn_mapping=table_alias_to_urn if replace_aliases else None,
                                override_dialect=override_dialect,
                            )
                        except Exception as e:
                            logger.debug(f"Failed to enhance transformation logic: {e}")
                            enhanced_logic = raw_logic
                    else:
                        enhanced_logic = raw_logic

                    # Format the transformation operation
                    if col_lineage.logic.is_direct_copy:
                        transform_operation = f"COPY: {enhanced_logic}"
                    else:
                        transform_operation = f"SQL: {enhanced_logic}"

                fine_grained_lineages.append(
                    models.FineGrainedLineageClass(
                        upstreamType=models.FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=[
                            SchemaFieldUrn(upstream_table, ref.column).urn()
                            for ref in upstream_refs
                        ],
                        downstreamType=models.FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[
                            SchemaFieldUrn(
                                downstream_urn, col_lineage.downstream.column
                            ).urn()
                        ],
                        transformOperation=transform_operation,
                        query=query_urn,
                        confidenceScore=parsed_result.debug_info.confidence,
                    )
                )

        # Build dataset patch
        updater = DatasetPatchBuilder(str(downstream_urn))
        updater.add_upstream_lineage(
            models.UpstreamClass(
                dataset=str(upstream_table),
                type=models.DatasetLineageTypeClass.TRANSFORMED,
                query=query_urn,
            )
        )

        for fgl in fine_grained_lineages:
            updater.add_fine_grained_upstream_lineage(fgl)

        if not actual_graph.exists(updater.urn):
            logger.warning(
                f"Dataset {updater.urn} does not exist. Creating lineage anyway."
            )

        mcps: List[
            Union[
                MetadataChangeProposalWrapper,
                models.MetadataChangeProposalClass,
            ]
        ] = list(updater.build())

        actual_graph.emit_mcps(mcps)

    # Emit query entity
    if query_entity:
        actual_graph.emit_mcps(query_entity)

    logger.info(
        f"Successfully created enhanced lineage for {downstream_urn} "
        f"with {len(parsed_result.in_tables)} upstream table(s) "
        f"and {len(parsed_result.column_lineage or [])} column lineage relationship(s)"
    )
