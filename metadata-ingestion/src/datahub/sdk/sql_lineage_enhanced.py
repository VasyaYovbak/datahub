"""
Enhanced SQL lineage utilities with CTE expansion and alias resolution - IMPROVED VERSION.

This module provides advanced transformation logic extraction that:
- Extracts CTEs from optimized SQL (including sqlglot's internal CTEs)
- Expands CTE column references to their actual calculations
- Replaces table aliases with full table URNs
- Suppresses sqlglot optimizer warnings
- Provides readable transformation logic even for complex correlated subqueries
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Union

import sqlglot
import sqlglot.expressions as exp
import sqlglot.optimizer

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

# Suppress sqlglot optimizer warnings
sqlglot_logger = logging.getLogger("sqlglot")
original_sqlglot_level = sqlglot_logger.level

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


def extract_ctes_from_optimized_sql(
    statement: sqlglot.exp.Expression,
    dialect: sqlglot.Dialect,
) -> Dict[str, CTEDefinition]:
    """
    Extract CTE definitions from an optimized SQL statement.

    This captures both explicit CTEs and internal CTEs created by sqlglot's optimizer
    when it converts correlated subqueries.
    """
    ctes = {}

    # Find all CTEs in the optimized statement
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
            for idx, select_col in enumerate(cte_query.expressions):
                col_name = select_col.alias_or_name

                # Skip * wildcards
                if col_name == "*":
                    continue

                # For unnamed columns, use positional naming like sqlglot does (_col_0, _col_1, etc.)
                if not col_name:
                    col_name = f"_col_{idx}"

                # Get the expression for this column
                if isinstance(select_col, exp.Alias):
                    col_expr = select_col.this
                else:
                    col_expr = select_col

                # Store the calculation
                column_mappings[col_name] = col_expr.sql(dialect=dialect)

        ctes[cte_name] = CTEDefinition(
            name=cte_name,
            select_expression=cte_query.sql(dialect=dialect),
            column_mappings=column_mappings,
        )

    # Also look for derived tables in JOINs (these are like inline CTEs)
    for join in statement.find_all(exp.Join):
        if isinstance(join.this, exp.Subquery):
            subquery_alias = join.this.alias
            subquery_expr = join.this.this

            if subquery_alias and isinstance(subquery_expr, exp.Select):
                column_mappings = {}
                for idx, select_col in enumerate(subquery_expr.expressions):
                    col_name = select_col.alias_or_name

                    # Skip * wildcards
                    if col_name == "*":
                        continue

                    # For unnamed columns, use positional naming like sqlglot does (_col_0, _col_1, etc.)
                    if not col_name:
                        col_name = f"_col_{idx}"

                    if isinstance(select_col, exp.Alias):
                        col_expr = select_col.this
                    else:
                        col_expr = select_col

                    column_mappings[col_name] = col_expr.sql(dialect=dialect)

                # Store as a "virtual CTE"
                ctes[subquery_alias] = CTEDefinition(
                    name=subquery_alias,
                    select_expression=subquery_expr.sql(dialect=dialect),
                    column_mappings=column_mappings,
                )

    return ctes


def expand_cte_references_recursively(
    transformation_logic: str,
    cte_definitions: Dict[str, CTEDefinition],
    dialect: sqlglot.Dialect,
    max_depth: int = 5,
) -> str:
    """
    Recursively expand CTE column references to their actual calculations.

    This handles nested CTEs where one CTE references another.
    """
    if not cte_definitions or max_depth <= 0:
        return transformation_logic

    # Parse the transformation logic
    try:
        expr = sqlglot.parse_one(f"SELECT {transformation_logic}", dialect=dialect)
    except Exception as e:
        logger.debug(f"Failed to parse transformation logic for CTE expansion: {e}")
        return transformation_logic

    changed = False

    # Find all column references
    for col_ref in list(expr.find_all(exp.Column)):
        table_alias = col_ref.table
        col_name = col_ref.name

        logger.debug(f"    Found reference: {table_alias}.{col_name}")

        # Check if this references a CTE (including internal ones like _u_0, _u_1)
        if table_alias and table_alias in cte_definitions:
            cte_def = cte_definitions[table_alias]
            logger.debug(f"      → CTE '{table_alias}' found, has columns: {list(cte_def.column_mappings.keys())}")

            if col_name in cte_def.column_mappings:
                calculation = cte_def.column_mappings[col_name]
                logger.debug(f"      → Expanding '{col_name}' to: {calculation}")

                try:
                    calc_expr = sqlglot.parse_one(f"SELECT {calculation}", dialect=dialect)
                    if isinstance(calc_expr, exp.Select) and calc_expr.expressions:
                        replacement_expr = calc_expr.expressions[0]

                        # Remove alias if present
                        if isinstance(replacement_expr, exp.Alias):
                            replacement_expr = replacement_expr.this

                        # Replace the column reference
                        col_ref.replace(replacement_expr.copy())
                        changed = True
                        logger.debug(f"      ✅ Replaced successfully")
                except Exception as e:
                    logger.debug(f"      ❌ Failed to expand: {e}")
            else:
                logger.debug(f"      ⚠️ Column '{col_name}' not found in CTE '{table_alias}'")
        else:
            if table_alias:
                logger.debug(f"      ⚠️ Table '{table_alias}' is not a CTE")

    # Extract the result
    if isinstance(expr, exp.Select) and expr.expressions:
        result = expr.expressions[0]

        # Remove any alias
        if isinstance(result, exp.Alias):
            result = result.this

        result_sql = result.sql(dialect=dialect)
    else:
        result_sql = expr.sql(dialect=dialect)

    # If we made changes, recursively expand again (for nested CTEs)
    if changed and max_depth > 1:
        return expand_cte_references_recursively(
            result_sql, cte_definitions, dialect, max_depth - 1
        )

    return result_sql


def replace_table_aliases_with_names(
    transformation_logic: str,
    table_alias_to_urn_mapping: Dict[str, str],
    dialect: sqlglot.Dialect,
) -> str:
    """
    Replace table aliases with readable table names from URNs.
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

            try:
                dataset_urn = DatasetUrn.from_string(urn_str)
                # Extract just the table name
                table_name_parts = dataset_urn.name.split(".")
                table_name = table_name_parts[-1] if table_name_parts else dataset_urn.name
                col_ref.set("table", table_name)
            except Exception:
                pass

    # Extract result
    if isinstance(expr, exp.Select) and expr.expressions:
        result = expr.expressions[0]
        if isinstance(result, exp.Alias):
            result = result.this
        result_sql = result.sql(dialect=dialect)
    else:
        result_sql = expr.sql(dialect=dialect)

    return result_sql


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
    suppress_warnings: bool = True,
) -> None:
    """
    Add lineage with enhanced transformation logic that expands CTEs and replaces aliases.

    This improved version:
    - Extracts CTEs from the OPTIMIZED SQL (including sqlglot's internal CTEs)
    - Recursively expands nested CTE references
    - Replaces table aliases with readable names
    - Suppresses sqlglot optimizer warnings

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
        replace_aliases: Whether to replace table aliases (default: True)
        suppress_warnings: Whether to suppress sqlglot warnings (default: True)
    """
    from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result

    # Suppress sqlglot warnings if requested
    if suppress_warnings:
        sqlglot_logger.setLevel(logging.ERROR)

    try:
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
                f"Failed to parse column-level lineage: {parsed_result.debug_info.error}",
            )

        if not parsed_result.out_tables:
            raise SdkUsageError(
                "No output tables found in the query. Cannot establish lineage."
            )

        downstream_urn = parsed_result.out_tables[0]

        # Get the optimized statement to extract CTEs from
        dialect = get_dialect(override_dialect or platform)

        # Parse and optimize the SQL to get internal CTEs
        statement = sqlglot.parse_one(query_text, dialect=dialect)

        # Apply the same optimizations that DataHub's parser uses
        # Use the same optimization rules as DataHub (qualify + unnest_subqueries)
        _OPTIMIZE_RULES = (
            sqlglot.optimizer.optimizer.qualify,
            sqlglot.optimizer.optimizer.unnest_subqueries,
        )

        try:
            optimized_statement = sqlglot.optimizer.optimizer.optimize(
                statement.copy(),
                dialect=dialect,
                schema=None,  # We don't have schema info, but that's OK for structure
                qualify_columns=False,  # Don't qualify columns without schema
                validate_qualify_columns=False,
                allow_partial_qualification=True,
                identify=False,
                catalog=default_db,
                db=default_schema,
                rules=_OPTIMIZE_RULES,
            )
            logger.debug("✅ Successfully optimized SQL with unnest_subqueries")
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Optimized SQL:\n{optimized_statement.sql(pretty=True, dialect=dialect)}")
        except Exception as e:
            logger.debug(f"⚠️ Optimization failed, using basic qualify: {e}")
            # Fallback to basic qualify if optimize fails
            optimized_statement = sqlglot.optimizer.qualify.qualify(
                statement.copy(),
                dialect=dialect,
                catalog=default_db,
                db=default_schema,
                qualify_columns=False,
                validate_qualify_columns=False,
                allow_partial_qualification=True,
                identify=False,
            )

        # Extract CTE definitions from the optimized statement
        cte_definitions = {}
        if expand_ctes:
            cte_definitions = extract_ctes_from_optimized_sql(optimized_statement, dialect)
            logger.info(f"🔍 Extracted {len(cte_definitions)} CTE definitions: {list(cte_definitions.keys())}")

            # Debug: Show CTE contents
            for cte_name, cte_def in cte_definitions.items():
                logger.info(f"  CTE '{cte_name}' columns: {list(cte_def.column_mappings.keys())}")
                for col_name, col_expr in cte_def.column_mappings.items():
                    logger.info(f"    {col_name} = {col_expr[:100]}..." if len(col_expr) > 100 else f"    {col_name} = {col_expr}")

        # Build table alias to URN mapping
        table_alias_to_urn: Dict[str, str] = {}
        if replace_aliases:
            try:
                for table_ref in statement.find_all(exp.Table):
                    table_alias = table_ref.alias_or_name
                    table_name = table_ref.name

                    for urn in parsed_result.in_tables:
                        if table_name.lower() in urn.lower():
                            table_alias_to_urn[table_alias] = urn
                            break
            except Exception as e:
                logger.debug(f"Failed to extract table aliases: {e}")

        # Create query entity
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
                        enhanced_logic = raw_logic

                        logger.info(f"\n📊 Processing column: {col_lineage.downstream.column}")
                        logger.info(f"  Raw logic: {raw_logic}")

                        # Step 1: Expand CTE references
                        if expand_ctes and cte_definitions:
                            try:
                                enhanced_logic = expand_cte_references_recursively(
                                    enhanced_logic,
                                    cte_definitions,
                                    dialect,
                                    max_depth=5,
                                )
                                if enhanced_logic != raw_logic:
                                    logger.info(f"  After CTE expansion: {enhanced_logic}")
                                else:
                                    logger.info(f"  ⚠️ CTE expansion: no changes made")
                            except Exception as e:
                                logger.warning(f"  ❌ Failed to expand CTEs: {e}")

                        # Step 2: Replace table aliases
                        if replace_aliases and table_alias_to_urn:
                            try:
                                before_alias_replacement = enhanced_logic
                                enhanced_logic = replace_table_aliases_with_names(
                                    enhanced_logic,
                                    table_alias_to_urn,
                                    dialect,
                                )
                                if enhanced_logic != before_alias_replacement:
                                    logger.info(f"  After alias replacement: {enhanced_logic}")
                            except Exception as e:
                                logger.warning(f"  ❌ Failed to replace aliases: {e}")

                        logger.info(f"  ✅ Final: {enhanced_logic}")

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

    finally:
        # Restore original logging level
        if suppress_warnings:
            sqlglot_logger.setLevel(original_sqlglot_level)
