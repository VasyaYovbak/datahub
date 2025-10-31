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
import re
import uuid
from dataclasses import dataclass, field
from enum import Enum
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
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.dataset import Dataset
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

    def __init__(
        self, name: str, select_expression: str, column_mappings: Dict[str, str]
    ):
        self.name = name
        self.select_expression = select_expression
        self.column_mappings = column_mappings  # output_col -> calculation


class NodeType(Enum):
    """Types of nodes in a procedure execution flow."""

    PROCEDURE_START = "procedure_start"
    CREATE_TEMP_TABLE = "create_temp_table"
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    MERGE = "merge"
    TRUNCATE = "truncate"
    SELECT_INTO = "select_into"
    UNKNOWN = "unknown"


@dataclass
class TempTableInfo:
    """Information about a temporary table created during procedure execution."""

    table_name: str
    columns: Dict[str, str] = field(default_factory=dict)  # column_name -> source_expr
    created_in_node_id: str = ""
    dataset_urn: Optional[str] = None
    column_lineage: Optional[SqlParsingResult] = None


@dataclass
class ProcedureNode:
    """Represents a single operation node in a SQL procedure."""

    node_id: str
    node_type: NodeType
    sql_text: str
    sequence_order: int
    lineage_result: Optional[SqlParsingResult] = None
    created_temp_tables: List[str] = field(default_factory=list)
    used_temp_tables: List[str] = field(default_factory=list)
    upstream_nodes: List[str] = field(default_factory=list)
    downstream_nodes: List[str] = field(default_factory=list)


class TempTableTracker:
    """Tracks temporary tables throughout procedure execution."""

    def __init__(self):
        self.temp_tables: Dict[str, TempTableInfo] = {}
        self._table_name_variations: Dict[str, str] = {}

    def register_temp_table(
        self,
        table_name: str,
        columns: Dict[str, str],
        created_in_node_id: str,
        dataset_urn: Optional[str] = None,
    ) -> None:
        """Register a new temporary table."""
        normalized_name = table_name.lower()
        self.temp_tables[normalized_name] = TempTableInfo(
            table_name=table_name,
            columns=columns,
            created_in_node_id=created_in_node_id,
            dataset_urn=dataset_urn,
        )
        self._table_name_variations[normalized_name] = table_name
        logger.info(
            f"📝 Registered temp table '{table_name}' with {len(columns)} columns"
        )

    def is_temp_table(self, table_name: str) -> bool:
        """Check if a table is a registered temporary table."""
        return table_name.lower() in self.temp_tables

    def get_temp_table(self, table_name: str) -> Optional[TempTableInfo]:
        """Get information about a temporary table."""
        return self.temp_tables.get(table_name.lower())

    def resolve_column_source(
        self, table_name: str, column_name: str
    ) -> Optional[str]:
        """Resolve the source expression for a temp table column."""
        temp_table = self.get_temp_table(table_name)
        if temp_table and column_name in temp_table.columns:
            return temp_table.columns[column_name]
        return None

    def set_column_lineage(
        self, table_name: str, lineage_result: SqlParsingResult
    ) -> None:
        """Store the column lineage result for a temp table."""
        temp_table = self.get_temp_table(table_name)
        if temp_table:
            temp_table.column_lineage = lineage_result


def assign_anonymous_projection_aliases(
    expression: sqlglot.exp.Expression,
) -> sqlglot.exp.Expression:
    """
    Assign default aliases to anonymous projections in SELECT statements.
    Mimics DataHub's behavior to ensure consistent referencing.
    """
    for select in expression.find_all(exp.Select):
        for i, projection in enumerate(select.expressions):
            if not projection.alias:
                projection.set("alias", exp.to_identifier(f"_col_{i}"))
    return expression


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
        print(f"HERE IS SOME CTE DATA: {cte}")
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
                    # Get the expression for this column
                    if isinstance(select_col, exp.Alias):
                        col_expr = select_col.this
                    else:
                        col_expr = select_col

                    column_mappings[col_name] = col_expr.sql(dialect=dialect)

        ctes[cte_name] = CTEDefinition(
            name=cte_name,
            select_expression=cte_query.sql(dialect=dialect),
            column_mappings=column_mappings,
        )

    # Also look for derived tables in JOINs (these are like inline CTEs)
    for join in statement.find_all(exp.Join, bfs=False):
        print(f"HERE IS SOME join DATA: {join}")
        if isinstance(join.this, exp.Subquery):
            subquery_alias = join.this.alias
            subquery_expr = join.this.this

            if subquery_alias and isinstance(subquery_expr, exp.Select):
                column_mappings = {}
                for select_col in subquery_expr.expressions:
                    col_name = select_col.alias_or_name
                    if col_name and col_name != "*":
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


def check_if_provided_name_is_columns_of_another_cte(
    col_name: str, cte_definitions: Dict[str, CTEDefinition]
):
    for cte_def in cte_definitions.values():
        if col_name in cte_def.column_mappings:
            return True

    return False


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
            logger.debug(
                f"      → CTE '{table_alias}' found, has columns: {list(cte_def.column_mappings.keys())}"
            )

            if col_name in cte_def.column_mappings:
                calculation = cte_def.column_mappings[col_name]
                logger.debug(f"      → Expanding '{col_name}' to: {calculation}")

                try:
                    calc_expr = sqlglot.parse_one(
                        f"SELECT {calculation}", dialect=dialect
                    )
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
                logger.debug(
                    f"      ⚠️ Column '{col_name}' not found in CTE '{table_alias}'"
                )
        elif check_if_provided_name_is_columns_of_another_cte(
            table_alias, cte_definitions
        ):
            for cte_def in cte_definitions.values():
                if table_alias in cte_def.column_mappings:
                    calculation = cte_def.column_mappings[table_alias]
                    logger.debug(f"      → Expanding '{table_alias}' to: {calculation}")
                    try:
                        calc_expr = sqlglot.parse_one(
                            f"SELECT {calculation}", dialect=dialect
                        )
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
                table_name = (
                    table_name_parts[-1] if table_name_parts else dataset_urn.name
                )
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


def process_statement_as_datahub(
    sql,
    platform: str,
    env: str,
    graph: DataHubGraph | None = None,
    platform_instance: str | None = None,
    schema_aware: bool = True,
    default_db: str | None = None,
    default_schema: str | None = None,
):
    from datahub.sql_parsing.sqlglot_lineage import (
        _normalize_db_or_schema,
        parse_statement,
        _simplify_select_into,
        _table_level_lineage,
        _TableName,
        SchemaInfo,
        create_schema_resolver,
        _prepare_query_columns,
        _try_extract_select,
        get_dialect,
        SQL_PARSER_TRACE,
        cooperative_timeout,
        SQL_LINEAGE_TIMEOUT_SECONDS,
        SQL_LINEAGE_TIMEOUT_ENABLED,
        sqlglot,
    )

    schema_resolver = create_schema_resolver(
        platform=platform,
        platform_instance=platform_instance,
        env=env,
        schema_aware=schema_aware,
        graph=graph,
    )
    dialect = get_dialect(schema_resolver.platform)

    default_db = _normalize_db_or_schema(default_db, dialect)
    default_schema = _normalize_db_or_schema(default_schema, dialect)

    logger.debug("Parsing lineage from sql statement: %s", sql)
    statement = parse_statement(sql, dialect=dialect)

    original_statement, statement = statement, statement.copy()
    statement = _simplify_select_into(statement)

    statement = sqlglot.optimizer.qualify.qualify(
        statement,
        dialect=dialect,
        catalog=default_db,
        db=default_schema,
        qualify_columns=False,
        validate_qualify_columns=False,
        allow_partial_qualification=True,
        identify=False,
    )
    tables, modified = _table_level_lineage(statement, dialect=dialect)
    table_name_urn_mapping: Dict[_TableName, str] = {}
    table_name_schema_mapping: Dict[_TableName, SchemaInfo] = {}

    for table in tables | modified:
        qualified_table = table.qualified(
            dialect=dialect, default_db=default_db, default_schema=default_schema
        )
        urn, schema_info = schema_resolver.resolve_table(qualified_table)
        table_name_urn_mapping[qualified_table] = urn
        if schema_info:
            table_name_schema_mapping[qualified_table] = schema_info
        table_name_urn_mapping[table] = urn

    total_tables_discovered = len(tables | modified)
    total_schemas_resolved = len(table_name_schema_mapping)
    logger.debug(
        f"Resolved {total_schemas_resolved} of {total_tables_discovered} table schemas"
    )
    if SQL_PARSER_TRACE:
        for qualified_table, schema_info in table_name_schema_mapping.items():
            logger.debug(
                "Table name %s resolved to %s with schema %s",
                qualified_table,
                table_name_urn_mapping[qualified_table],
                schema_info,
            )

    with cooperative_timeout(
        timeout=(SQL_LINEAGE_TIMEOUT_SECONDS if SQL_LINEAGE_TIMEOUT_ENABLED else None)
    ):
        select_statement = _try_extract_select(statement)

        (select_statement, column_resolver) = _prepare_query_columns(
            select_statement,
            dialect=dialect,
            table_schemas=table_name_schema_mapping,
            default_db=default_db,
            default_schema=default_schema,
        )

    return select_statement


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

        # Parse the SQL
        statement = sqlglot.parse_one(query_text, dialect=dialect)

        optimized_statement = process_statement_as_datahub(
            query_text,
            platform,
            env,
            actual_graph,
            platform_instance,
            True,
            default_db,
            default_schema,
        )
        cte_definitions = {}
        if expand_ctes:
            cte_definitions = extract_ctes_from_optimized_sql(
                optimized_statement, dialect
            )
            logger.info(
                f"🔍 Extracted {len(cte_definitions)} CTE definitions: {list(cte_definitions.keys())}"
            )

            # Debug: Show CTE contents
            for cte_name, cte_def in cte_definitions.items():
                logger.info(
                    f"  CTE '{cte_name}' columns: {list(cte_def.column_mappings.keys())}"
                )
                for col_name, col_expr in cte_def.column_mappings.items():
                    logger.info(
                        f"    {col_name} = {col_expr[:100]}..."
                        if len(col_expr) > 100
                        else f"    {col_name} = {col_expr}"
                    )

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

                        logger.info(
                            f"\n📊 Processing column: {col_lineage.downstream.column}"
                        )
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
                                    logger.info(
                                        f"  After CTE expansion: {enhanced_logic}"
                                    )
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
                                    logger.info(
                                        f"  After alias replacement: {enhanced_logic}"
                                    )
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


def _extract_statements_from_text(procedure_sql: str, dialect: str) -> List[str]:
    """
    Extract SQL statements from procedure text using regex.

    This is a fallback when sqlglot cannot parse the procedure structure.
    Extracts common DML/DDL statements from the procedure body.
    Returns statements in the order they appear in the original code.
    """
    procedure_body = procedure_sql
    begin_match = re.search(r'\bBEGIN\b', procedure_sql, re.IGNORECASE)
    end_match = re.search(r'\bEND\s*;?\s*\$\$', procedure_sql, re.IGNORECASE)

    if begin_match and end_match:
        procedure_body = procedure_sql[begin_match.end() : end_match.start()]
        logger.debug(f"Extracted procedure body between BEGIN and END")

    patterns = [
        (r'TRUNCATE\s+TABLE\s+[\w.]+\s*;', NodeType.TRUNCATE),
        (
            r'CREATE\s+(?:TEMP|TEMPORARY)\s+TABLE\s+[\w.]+\s+AS\s+SELECT\s+.+?(?=(?:CREATE|INSERT|UPDATE|DELETE|MERGE|TRUNCATE|GET\s+DIAGNOSTICS|RETURN|\Z))',
            NodeType.CREATE_TEMP_TABLE,
        ),
        (
            r'INSERT\s+INTO\s+[\w.]+\s*\([^)]*\)\s+SELECT\s+.+?(?=(?:CREATE|INSERT|UPDATE|DELETE|MERGE|TRUNCATE|GET\s+DIAGNOSTICS|RETURN|\Z))',
            NodeType.INSERT,
        ),
        (r'UPDATE\s+[\w.]+\s+SET\s+.+?;', NodeType.UPDATE),
        (r'DELETE\s+FROM\s+[\w.]+\s+.+?;', NodeType.DELETE),
        (r'MERGE\s+INTO\s+[\w.]+\s+.+?;', NodeType.MERGE),
    ]

    all_matches = []
    for pattern, node_type in patterns:
        matches = re.finditer(pattern, procedure_body, re.IGNORECASE | re.DOTALL)
        for match in matches:
            all_matches.append((match.start(), match.group(0).strip(), node_type))

    all_matches.sort(key=lambda x: x[0])

    statements = []
    for start_pos, sql_text, node_type in all_matches:
        sql_text = re.sub(r'--[^\n]*', '', sql_text)
        sql_text = re.sub(r'/\*.*?\*/', ' ', sql_text, flags=re.DOTALL)
        sql_text = re.sub(r'\s+', ' ', sql_text)

        if not sql_text.endswith(';'):
            sql_text += ';'

        if sql_text and sql_text not in statements:
            statements.append(sql_text)
            logger.debug(f"  [{start_pos}] Extracted: {sql_text[:80]}...")

    logger.info(
        f"📝 Regex extracted {len(statements)} statements from procedure body (in order)"
    )
    return statements


def _parse_statements_with_regex_fallback(
    procedure_sql: str, dialect: str
) -> List[ProcedureNode]:
    """Parse procedure using regex fallback when sqlglot fails."""
    nodes: List[ProcedureNode] = []

    param_match = re.search(
        r'FUNCTION\s+\w+\s*\((.*?)\)', procedure_sql, re.IGNORECASE | re.DOTALL
    )
    if param_match:
        params_text = param_match.group(1).strip()
        if params_text:
            nodes.append(
                ProcedureNode(
                    node_id=f"node_0_start",
                    node_type=NodeType.PROCEDURE_START,
                    sql_text=f"-- Parameters: {params_text}",
                    sequence_order=0,
                )
            )

    statements_with_types = _extract_statements_from_text(procedure_sql, dialect)

    for i, sql_text in enumerate(statements_with_types, start=len(nodes)):
        sql_upper = sql_text.upper()

        if "TRUNCATE" in sql_upper:
            node_type = NodeType.TRUNCATE
        elif "CREATE" in sql_upper and ("TEMP" in sql_upper or "TEMPORARY" in sql_upper):
            node_type = NodeType.CREATE_TEMP_TABLE
        elif "INSERT" in sql_upper:
            node_type = NodeType.INSERT
        elif "UPDATE" in sql_upper:
            node_type = NodeType.UPDATE
        elif "DELETE" in sql_upper:
            node_type = NodeType.DELETE
        elif "MERGE" in sql_upper:
            node_type = NodeType.MERGE
        else:
            try:
                parsed_stmt = sqlglot.parse_one(sql_text, dialect=dialect)
                node_type = _classify_statement_type(parsed_stmt)
            except Exception:
                node_type = NodeType.UNKNOWN

        nodes.append(
            ProcedureNode(
                node_id=f"node_{i}",
                node_type=node_type,
                sql_text=sql_text,
                sequence_order=i,
            )
        )

    return nodes


def _classify_statement_type(statement: sqlglot.exp.Expression) -> NodeType:
    """Classify a SQL statement into a node type."""
    if isinstance(statement, exp.Create):
        if statement.args.get("temporary"):
            return NodeType.CREATE_TEMP_TABLE
        return NodeType.UNKNOWN
    elif isinstance(statement, exp.Insert):
        return NodeType.INSERT
    elif isinstance(statement, exp.Update):
        return NodeType.UPDATE
    elif isinstance(statement, exp.Delete):
        return NodeType.DELETE
    elif isinstance(statement, exp.Merge):
        return NodeType.MERGE
    elif isinstance(statement, exp.Command):
        sql_lower = statement.sql().lower()
        if "truncate" in sql_lower:
            return NodeType.TRUNCATE
        return NodeType.UNKNOWN
    else:
        return NodeType.UNKNOWN


def parse_procedure_to_nodes(
    procedure_sql: str,
    dialect: str,
    procedure_name: Optional[str] = None,
) -> List[ProcedureNode]:
    """
    Parse a SQL procedure into individual execution nodes.

    Extracts individual SQL statements from a procedure and classifies them.
    Handles PostgreSQL and Oracle procedure syntax.
    """
    nodes: List[ProcedureNode] = []
    sequence = 0

    try:
        parsed = sqlglot.parse(procedure_sql, dialect=dialect)
        if not parsed or len(parsed) == 0:
            logger.warning("Failed to parse procedure, treating as single statement")
            parsed = [sqlglot.parse_one(procedure_sql, dialect=dialect)]
    except Exception as e:
        logger.warning(f"Failed to parse procedure: {e}, treating as single statement")
        try:
            parsed = [sqlglot.parse_one(procedure_sql, dialect=dialect)]
        except Exception as e2:
            logger.error(f"Cannot parse procedure at all: {e2}")
            nodes.append(
                ProcedureNode(
                    node_id="node_0",
                    node_type=NodeType.UNKNOWN,
                    sql_text=procedure_sql,
                    sequence_order=0,
                )
            )
            return nodes

    for statement in parsed:
        if statement is None:
            continue

        if isinstance(statement, exp.Create):
            if statement.kind == "FUNCTION" or statement.kind == "PROCEDURE":
                param_names = _extract_function_parameters(statement)
                if param_names:
                    nodes.append(
                        ProcedureNode(
                            node_id=f"node_{sequence}_start",
                            node_type=NodeType.PROCEDURE_START,
                            sql_text=f"-- Parameters: {', '.join(param_names)}",
                            sequence_order=sequence,
                        )
                    )
                    sequence += 1

                if statement.expression:
                    inner_statements = _extract_statements_from_function_body(
                        statement.expression, dialect
                    )

                    for stmt in inner_statements:
                        node_type = _classify_statement_type(stmt)
                        nodes.append(
                            ProcedureNode(
                                node_id=f"node_{sequence}",
                                node_type=node_type,
                                sql_text=stmt.sql(dialect=dialect),
                                sequence_order=sequence,
                            )
                        )
                        sequence += 1
                else:
                    logger.warning(
                        f"CREATE {statement.kind} has no body, skipping extraction"
                    )
            else:
                node_type = _classify_statement_type(statement)
                nodes.append(
                    ProcedureNode(
                        node_id=f"node_{sequence}",
                        node_type=node_type,
                        sql_text=statement.sql(dialect=dialect),
                        sequence_order=sequence,
                    )
                )
                sequence += 1

        elif isinstance(statement, exp.Command):
            sql_text = statement.sql(dialect=dialect)
            node_type = _classify_statement_type(statement)
            nodes.append(
                ProcedureNode(
                    node_id=f"node_{sequence}",
                    node_type=node_type,
                    sql_text=sql_text,
                    sequence_order=sequence,
                )
            )
            sequence += 1

        else:
            node_type = _classify_statement_type(statement)
            nodes.append(
                ProcedureNode(
                    node_id=f"node_{sequence}",
                    node_type=node_type,
                    sql_text=statement.sql(dialect=dialect),
                    sequence_order=sequence,
                )
            )
            sequence += 1

    if not nodes:
        logger.warning(
            "No nodes extracted via sqlglot, trying regex fallback method..."
        )
        nodes = _parse_statements_with_regex_fallback(procedure_sql, dialect)

    if not nodes:
        logger.warning("Regex fallback also failed, creating single node")
        nodes.append(
            ProcedureNode(
                node_id="node_0",
                node_type=NodeType.UNKNOWN,
                sql_text=procedure_sql,
                sequence_order=0,
            )
        )

    logger.info(f"📦 Extracted {len(nodes)} nodes from procedure")
    for node in nodes:
        logger.info(f"  - Node {node.sequence_order}: {node.node_type.value}")

    return nodes


def _extract_function_parameters(create_func: exp.Create) -> List[str]:
    """Extract parameter names from a CREATE FUNCTION statement."""
    params = []
    if hasattr(create_func, "args") and "params" in create_func.args:
        for param in create_func.args["params"].expressions:
            if isinstance(param, exp.ColumnDef):
                params.append(param.name)
    return params


def _extract_statements_from_function_body(
    body: sqlglot.exp.Expression, dialect: str
) -> List[sqlglot.exp.Expression]:
    """Extract individual statements from a function body."""
    statements = []

    if isinstance(body, (exp.Select, exp.Insert, exp.Update, exp.Delete, exp.Merge)):
        statements.append(body)
    else:
        statement_types = (
            exp.Select,
            exp.Insert,
            exp.Update,
            exp.Delete,
            exp.Merge,
            exp.Create,
        )
        for stmt in body.find_all(statement_types):
            if stmt not in statements:
                statements.append(stmt)

    return statements


def _process_temp_table_creation_node(
    node: ProcedureNode,
    temp_tracker: TempTableTracker,
    platform: str,
    env: str,
    graph: DataHubGraph,
    platform_instance: Optional[str],
    default_db: Optional[str],
    default_schema: Optional[str],
    dialect: sqlglot.Dialect,
    expand_ctes: bool = True,
    replace_aliases: bool = True,
) -> None:
    """Process a CREATE TEMP TABLE node, create Dataset entity, and register temp table."""
    from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result

    try:
        statement = sqlglot.parse_one(node.sql_text, dialect=dialect)

        if not isinstance(statement, exp.Create):
            logger.warning(f"Node {node.node_id} is not a CREATE statement")
            return

        table_name = None
        if statement.this:
            if isinstance(statement.this, exp.Table):
                table_name = statement.this.name
            elif isinstance(statement.this, exp.Schema):
                if statement.this.this and isinstance(statement.this.this, exp.Table):
                    table_name = statement.this.this.name

        if not table_name:
            logger.warning(f"Could not extract table name from CREATE statement")
            return

        select_stmt = statement.expression
        if not select_stmt or not isinstance(select_stmt, exp.Select):
            logger.warning(f"CREATE TEMP TABLE without SELECT, skipping lineage")
            temp_tracker.register_temp_table(
                table_name=table_name,
                columns={},
                created_in_node_id=node.node_id,
            )
            node.created_temp_tables.append(table_name)
            return

        select_sql = select_stmt.sql(dialect=dialect)
        parsed_result: SqlParsingResult = create_lineage_sql_parsed_result(
            query=select_sql,
            default_db=default_db,
            default_schema=default_schema,
            platform=platform,
            platform_instance=platform_instance,
            env=env,
            graph=graph,
        )

        optimized_statement = process_statement_as_datahub(
            select_sql,
            platform,
            env,
            graph,
            platform_instance,
            True,
            default_db,
            default_schema,
        )

        cte_definitions = {}
        if expand_ctes:
            cte_definitions = extract_ctes_from_optimized_sql(
                optimized_statement, dialect
            )

        table_alias_to_urn: Dict[str, str] = {}
        if replace_aliases:
            try:
                for table_ref in statement.find_all(exp.Table):
                    table_alias = table_ref.alias_or_name
                    table_name_str = table_ref.name

                    for urn in parsed_result.in_tables:
                        if table_name_str.lower() in urn.lower():
                            table_alias_to_urn[table_alias] = urn
                            break
            except Exception as e:
                logger.debug(f"Failed to extract table aliases: {e}")

        columns = {}
        if parsed_result.column_lineage:
            for col_lineage in parsed_result.column_lineage:
                if col_lineage.downstream and col_lineage.downstream.column:
                    col_name = col_lineage.downstream.column
                    source_expr = (
                        col_lineage.logic.column_logic
                        if col_lineage.logic
                        else col_name
                    )

                    enhanced_logic = source_expr
                    if expand_ctes and cte_definitions:
                        try:
                            enhanced_logic = expand_cte_references_recursively(
                                enhanced_logic, cte_definitions, dialect, max_depth=5
                            )
                        except Exception as e:
                            logger.debug(f"Failed to expand CTEs for {col_name}: {e}")

                    if replace_aliases and table_alias_to_urn:
                        try:
                            enhanced_logic = replace_table_aliases_with_names(
                                enhanced_logic, table_alias_to_urn, dialect
                            )
                        except Exception as e:
                            logger.debug(f"Failed to replace aliases for {col_name}: {e}")

                    columns[col_name] = enhanced_logic

        temp_table_urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{default_db}.{default_schema}.{table_name},{env})"

        temp_dataset = Dataset(
            platform=platform,
            name=f"{default_db}.{default_schema}.{table_name}",
            env=env,
            platform_instance=platform_instance,
        )

        schema_fields = []
        for col_name, source_expr in columns.items():
            schema_field = models.SchemaFieldClass(
                fieldPath=col_name,
                type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                nativeDataType="TEXT",
                description=f"Source: {source_expr}",
            )
            schema_fields.append(schema_field)

        if schema_fields:
            schema_metadata = models.SchemaMetadataClass(
                schemaName=table_name,
                platform=f"urn:li:dataPlatform:{platform}",
                version=0,
                hash="",
                fields=schema_fields,
                platformSchema=models.OtherSchemaClass(rawSchema=""),
            )
            temp_dataset._set_aspect(schema_metadata)

        for mcp in temp_dataset.as_mcps():
            graph.emit_mcp(mcp)

        temp_tracker.register_temp_table(
            table_name=table_name,
            columns=columns,
            created_in_node_id=node.node_id,
            dataset_urn=temp_table_urn,
        )
        temp_tracker.set_column_lineage(table_name, parsed_result)

        node.created_temp_tables.append(table_name)
        node.lineage_result = parsed_result

        logger.info(
            f"✅ Created Dataset and processed CREATE TEMP TABLE '{table_name}' with {len(columns)} columns"
        )

    except Exception as e:
        logger.error(f"Failed to process temp table creation: {e}", exc_info=True)


def process_procedure_lineage(
    *,
    graph: Union[DataHubGraph, DataHubClient],
    procedure_sql: str,
    procedure_name: str,
    platform: str,
    platform_instance: Optional[str] = None,
    env: str = "PROD",
    default_db: Optional[str] = None,
    default_schema: Optional[str] = None,
    procedure_parameters: Optional[Dict[str, str]] = None,
    override_dialect: Optional[str] = None,
    expand_ctes: bool = True,
    replace_aliases: bool = True,
    suppress_warnings: bool = True,
) -> None:
    """
    Process lineage for an entire SQL procedure with multiple operations.

    Creates a DataFlow representing the procedure and DataJob nodes for each operation.
    Tracks temporary tables across operations and resolves their lineage.

    Args:
        graph: DataHubGraph or DataHubClient instance
        procedure_sql: Full SQL procedure code
        procedure_name: Name of the procedure (for DataFlow)
        platform: Data platform identifier
        platform_instance: Optional platform instance
        env: Environment (default: "PROD")
        default_db: Default database name
        default_schema: Default schema name
        procedure_parameters: Optional dict of procedure parameters
        override_dialect: Optional dialect override
        expand_ctes: Whether to expand CTE references (default: True)
        replace_aliases: Whether to replace table aliases (default: True)
        suppress_warnings: Whether to suppress sqlglot warnings (default: True)
    """
    if suppress_warnings:
        sqlglot_logger.setLevel(logging.ERROR)

    try:
        if isinstance(graph, DataHubClient):
            actual_graph = graph._graph
        else:
            actual_graph = graph

        dialect = get_dialect(override_dialect or platform)

        logger.info(f"🚀 Processing procedure: {procedure_name}")

        nodes = parse_procedure_to_nodes(procedure_sql, dialect, procedure_name)
        temp_tracker = TempTableTracker()

        flow = DataFlow(
            platform="sql_procedure",
            name=procedure_name,
            platform_instance=platform_instance,
            env=env,
            description=f"SQL Procedure: {procedure_name}",
            subtype="SQL_PROCEDURE",
        )

        logger.info(f"📊 Created DataFlow for procedure: {flow.urn}")

        jobs = []
        for node in nodes:
            logger.info(
                f"\n{'='*60}\n🔄 Processing node {node.sequence_order}: {node.node_type.value}\n{'='*60}"
            )

            if node.node_type == NodeType.PROCEDURE_START:
                job = DataJob(
                    name=f"{procedure_name}_start",
                    flow=flow,
                    description=f"Procedure start - {node.sql_text}",
                )
                jobs.append(job)
                continue

            if node.node_type == NodeType.CREATE_TEMP_TABLE:
                _process_temp_table_creation_node(
                    node,
                    temp_tracker,
                    platform,
                    env,
                    actual_graph,
                    platform_instance,
                    default_db,
                    default_schema,
                    dialect,
                    expand_ctes=expand_ctes,
                    replace_aliases=replace_aliases,
                )

                job = DataJob(
                    name=f"{procedure_name}_node_{node.sequence_order}",
                    flow=flow,
                    description=f"Create temp table - {node.created_temp_tables}",
                )

                if node.lineage_result:
                    for upstream_table in node.lineage_result.in_tables:
                        job.set_inlets([upstream_table])

                    if node.created_temp_tables:
                        temp_info = temp_tracker.get_temp_table(
                            node.created_temp_tables[0]
                        )
                        if temp_info and temp_info.dataset_urn:
                            job.set_outlets([temp_info.dataset_urn])

                jobs.append(job)

            elif node.node_type in (
                NodeType.INSERT,
                NodeType.UPDATE,
                NodeType.DELETE,
                NodeType.MERGE,
            ):
                try:
                    infer_lineage_from_sql_with_enhanced_transformation_logic(
                        graph=actual_graph,
                        query_text=node.sql_text,
                        platform=platform,
                        platform_instance=platform_instance,
                        env=env,
                        default_db=default_db,
                        default_schema=default_schema,
                        override_dialect=override_dialect,
                        expand_ctes=expand_ctes,
                        replace_aliases=replace_aliases,
                        suppress_warnings=suppress_warnings,
                    )

                    job = DataJob(
                        name=f"{procedure_name}_node_{node.sequence_order}",
                        flow=flow,
                        description=f"{node.node_type.value.upper()} operation",
                    )
                    jobs.append(job)

                except Exception as e:
                    logger.warning(
                        f"Failed to process lineage for node {node.sequence_order}: {e}"
                    )

            elif node.node_type == NodeType.TRUNCATE:
                job = DataJob(
                    name=f"{procedure_name}_node_{node.sequence_order}",
                    flow=flow,
                    description=f"TRUNCATE operation",
                )
                jobs.append(job)

        logger.info(f"\n{'='*60}")
        logger.info(f"✅ Successfully processed procedure '{procedure_name}'")
        logger.info(f"   - Created DataFlow: {flow.urn}")
        logger.info(f"   - Created {len(jobs)} DataJob nodes")
        logger.info(f"   - Tracked {len(temp_tracker.temp_tables)} temp tables")
        logger.info(f"{'='*60}\n")

        for mcp in flow.as_mcps():
            actual_graph.emit_mcp(mcp)

        for job in jobs:
            for mcp in job.as_mcps():
                actual_graph.emit_mcp(mcp)

    finally:
        if suppress_warnings:
            sqlglot_logger.setLevel(original_sqlglot_level)
