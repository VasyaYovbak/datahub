"""Utilities for SQL lineage with enhanced transformation logic preservation."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Optional, Union

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.errors import SdkUsageError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import DatasetUrn, QueryUrn, SchemaFieldUrn
from datahub.sdk._utils import DEFAULT_ACTOR_URN
from datahub.sdk.main_client import DataHubClient
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.sql_parsing.fingerprint_utils import generate_hash
from datahub.utilities.ordered_set import OrderedSet

if TYPE_CHECKING:
    from datahub.sql_parsing.sqlglot_lineage import SqlParsingResult

logger = logging.getLogger(__name__)

_empty_audit_stamp = models.AuditStampClass(
    time=0,
    actor=DEFAULT_ACTOR_URN,
)


def infer_lineage_from_sql_with_transformation_logic(
    *,
    graph: Union[DataHubGraph, DataHubClient],
    query_text: str,
    platform: str,
    platform_instance: Optional[str] = None,
    env: str = "PROD",
    default_db: Optional[str] = None,
    default_schema: Optional[str] = None,
    override_dialect: Optional[str] = None,
) -> None:
    """
    Add lineage by parsing a SQL query with full transformation logic preservation.

    This function is an enhanced version of the standard infer_lineage_from_sql that
    preserves the transformation logic (SQL expressions) for each column lineage relationship.

    The transformation logic is stored in the `transformOperation` field of the fine-grained
    lineage and can be viewed in the DataHub UI to understand how each column is derived.

    Args:
        graph: DataHubGraph or DataHubClient instance for accessing the DataHub server
        query_text: The SQL query to parse for lineage extraction
        platform: Data platform identifier (e.g., "postgres", "snowflake", "bigquery")
        platform_instance: Optional platform instance identifier
        env: Environment identifier (default: "PROD")
        default_db: Default database name for resolving unqualified table references
        default_schema: Default schema name for resolving unqualified table references
        override_dialect: Optional SQLGlot dialect override for parsing

    Raises:
        SdkUsageError: If the SQL query cannot be parsed or no output tables are found

    Example:
        ```python
        from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
        from datahub.sdk.main_client import DataHubClient
        from datahub.sdk.sql_lineage_utils import infer_lineage_from_sql_with_transformation_logic

        gms_endpoint = "http://localhost:9007"
        graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))
        client = DataHubClient(graph=graph)

        sql = '''
        INSERT INTO staging_product_metrics (product_id, total_sold)
        SELECT p.product_id, COALESCE(SUM(oi.quantity), 0) AS total_sold
        FROM raw_products p
        LEFT JOIN raw_order_items oi ON oi.product_id = p.product_id
        '''

        infer_lineage_from_sql_with_transformation_logic(
            graph=client._graph,  # or just pass client, function handles both
            query_text=sql,
            platform="postgres",
            default_db="ecommerce",
            default_schema="public",
        )
        ```

    The resulting lineage will include:
    - Table-level lineage: raw_products → staging_product_metrics
    - Column-level lineage with transformation logic:
      - product_id: "COPY: p.product_id"
      - total_sold: "SQL: COALESCE(SUM(oi.quantity), 0)"
    """
    from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result

    # Handle both DataHubClient and DataHubGraph
    if isinstance(graph, DataHubClient):
        actual_graph = graph._graph
    else:
        actual_graph = graph

    # Parse the SQL query to extract lineage information
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

    # Use the first output table as the downstream
    downstream_urn = parsed_result.out_tables[0]

    # Create query URN and entity for the SQL statement
    query_urn = QueryUrn(generate_hash(query_text)).urn()
    from datahub.sql_parsing.sql_parsing_aggregator import make_query_subjects

    # Collect all fields involved in the lineage
    fields_involved = OrderedSet([str(downstream_urn)])
    for upstream_table in parsed_result.in_tables:
        if upstream_table != downstream_urn:
            fields_involved.add(str(upstream_table))

    # Add column fields if we have column lineage
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

    # Create the query entity with all subjects
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

    # Process each upstream table separately
    for upstream_table in parsed_result.in_tables:
        # Skip self-lineage
        if upstream_table == downstream_urn:
            continue

        # Build fine-grained lineage with transformation logic for this upstream table
        fine_grained_lineages: List[models.FineGrainedLineageClass] = []

        if parsed_result.column_lineage:
            for col_lineage in parsed_result.column_lineage:
                # Skip if no downstream column
                if not (col_lineage.downstream and col_lineage.downstream.column):
                    continue

                # Filter upstreams to only include columns from current upstream table
                upstream_refs = [
                    ref
                    for ref in col_lineage.upstreams
                    if ref.table == upstream_table and ref.column
                ]

                # Skip if no upstream columns from this table
                if not upstream_refs:
                    continue

                # Extract transformation logic from the parsed result
                transform_operation = None
                if col_lineage.logic:
                    # Format the transformation operation similar to sql_parsing_aggregator
                    if col_lineage.logic.is_direct_copy:
                        transform_operation = f"COPY: {col_lineage.logic.column_logic}"
                    else:
                        transform_operation = f"SQL: {col_lineage.logic.column_logic}"

                # Create fine-grained lineage with transformation logic
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
                        transformOperation=transform_operation,  # ✅ Logic preserved!
                        query=query_urn,
                        confidenceScore=parsed_result.debug_info.confidence,
                    )
                )

        # Build the dataset patch with lineage
        updater = DatasetPatchBuilder(str(downstream_urn))

        # Add table-level upstream lineage
        updater.add_upstream_lineage(
            models.UpstreamClass(
                dataset=str(upstream_table),
                type=models.DatasetLineageTypeClass.TRANSFORMED,
                query=query_urn,
            )
        )

        # Add fine-grained lineage with transformation logic
        for fgl in fine_grained_lineages:
            updater.add_fine_grained_upstream_lineage(fgl)

        # Check if dataset exists before updating
        if not actual_graph.exists(updater.urn):
            logger.warning(
                f"Dataset {updater.urn} does not exist. Creating lineage anyway, but the dataset should be created first."
            )

        # Emit metadata change proposals
        mcps: List[
            Union[
                MetadataChangeProposalWrapper,
                models.MetadataChangeProposalClass,
            ]
        ] = list(updater.build())

        actual_graph.emit_mcps(mcps)

    # Emit the query entity once after all upstream tables are processed
    if query_entity:
        actual_graph.emit_mcps(query_entity)

    logger.info(
        f"Successfully created lineage from SQL query for {downstream_urn} "
        f"with {len(parsed_result.in_tables)} upstream table(s) "
        f"and {len(parsed_result.column_lineage or [])} column lineage relationship(s) "
        f"with transformation logic preserved."
    )
