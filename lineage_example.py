import logging
from typing import List, Dict

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInputOutputClass,
)
from datahub.metadata.urns import DataFlowUrn, DataJobUrn
from datahub.sql_parsing.sqlglot_lineage import ColumnLineageInfo, SqlParsingResult

logging.basicConfig(level=logging.INFO)


def main() -> None:
    """
    Основна функція для демонстрації створення розширеного column-level lineage
    з правильним визначенням типів зв'язку (COPY/TRANSFORMED).
    """
    try:
        gms_endpoint: str = "http://localhost:8080"
        token: str = ""
        graph: DataHubGraph = DataHubGraph(
            DatahubClientConfig(server=gms_endpoint, token=token)
        )
        emitter: DatahubRestEmitter = DatahubRestEmitter(
            gms_server=gms_endpoint, token=token
        )
        logging.info("Successfully connected to DataHub.")
    except Exception as e:
        logging.error(f"Failed to connect to DataHub: {e}", exc_info=True)
        return

    postgres_function_logic: str = """
    INSERT INTO staging_product_metrics (
        product_id, product_name, category, current_price, avg_price_last_30d,
        total_sold, revenue_generated, profit_margin, stock_status, last_sale_date
    )
    SELECT
        p.product_id, p.product_name, p.category, p.base_price AS current_price,
        COALESCE((SELECT AVG(ph.new_price) FROM raw_price_history ph WHERE ph.product_id = p.product_id AND ph.changed_at > CURRENT_DATE - INTERVAL '30 days'), p.base_price) AS avg_price_last_30d,
        COALESCE((SELECT SUM(oi.quantity) FROM raw_order_items oi JOIN raw_orders o ON oi.order_id = o.order_id WHERE oi.product_id = p.product_id AND o.status IN ('shipped', 'delivered')), 0) AS total_sold,
        COALESCE((SELECT SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)) FROM raw_order_items oi JOIN raw_orders o ON oi.order_id = o.order_id WHERE oi.product_id = p.product_id AND o.status IN ('shipped', 'delivered')), 0) AS revenue_generated,
        CASE WHEN p.cost_price > 0 THEN ((p.base_price - p.cost_price) / p.base_price * 100) ELSE 0 END AS profit_margin,
        CASE WHEN p.stock_quantity = 0 THEN 'out_of_stock' WHEN p.stock_quantity < 10 THEN 'low_stock' ELSE 'in_stock' END AS stock_status,
        (SELECT MAX(o.order_date) FROM raw_order_items oi JOIN raw_orders o ON oi.order_id = o.order_id WHERE oi.product_id = p.product_id) AS last_sale_date
    FROM raw_products p;
    """
    platform: str = "postgres"
    env: str = "PROD"
    flow_id: str = "postgres_etl_pipelines"
    job_id: str = "calculate_staging_product_metrics"

    flow_urn = DataFlowUrn.create_from_ids(orchestrator="postgres", flow_id=flow_id, env=env)
    job_urn = DataJobUrn.create_from_ids(data_flow_urn=str(flow_urn), job_id=job_id)

    sql_parsing_result = graph.parse_sql_lineage(
        sql=postgres_function_logic,
        platform=platform,
        default_db="ecommerce",
        default_schema="public",
    )

    if sql_parsing_result.debug_info.error:
        logging.error(f"Failed to parse SQL: {sql_parsing_result.debug_info.error}")
        return

    in_tables = sql_parsing_result.in_tables
    out_tables = sql_parsing_result.out_tables

    if not out_tables:
        logging.error("No output table found. Cannot build lineage.")
        return

    mcps_to_emit: List[MetadataChangeProposalWrapper] = []

    job_io_aspect = DataJobInputOutputClass(inputDatasets=in_tables, outputDatasets=out_tables)
    mcps_to_emit.append(MetadataChangeProposalWrapper(entityUrn=str(job_urn), aspect=job_io_aspect))

    downstream_urn_str = out_tables[0]

    upstream_types: Dict[str, str] = {}
    if sql_parsing_result.column_lineage:
        is_transformed: Dict[str, bool] = {urn: False for urn in in_tables}
        for cl_info in sql_parsing_result.column_lineage:
            if cl_info.logic and not cl_info.logic.is_direct_copy:
                for upstream_col in cl_info.upstreams:
                    is_transformed[upstream_col.table] = True

        for urn in in_tables:
            upstream_types[urn] = "TRANSFORMED" if is_transformed[urn] else "COPY"

    upstreams = [
        Upstream(dataset=urn, type=upstream_types.get(urn, "TRANSFORMED"))
        for urn in in_tables
    ]

    fine_grained_lineages: List[FineGrainedLineage] = []
    if sql_parsing_result.column_lineage:
        for cl_info in sql_parsing_result.column_lineage:
            fine_grained_lineages.append(FineGrainedLineage(
                upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                upstreams=[make_schema_field_urn(up.table, up.column) for up in cl_info.upstreams],
                downstreamType=FineGrainedLineageDownstreamType.FIELD,
                downstreams=[make_schema_field_urn(cl_info.downstream.table, cl_info.downstream.column)],
                transformOperation=cl_info.logic.column_logic if cl_info.logic else None,
            ))

    downstream_lineage_aspect = UpstreamLineage(
        upstreams=upstreams,
        fineGrainedLineages=fine_grained_lineages,
    )
    mcps_to_emit.append(MetadataChangeProposalWrapper(entityUrn=downstream_urn_str, aspect=downstream_lineage_aspect))

    flow_info_aspect = DataFlowInfoClass(name=flow_id, customProperties={"sql_logic": postgres_function_logic})
    mcps_to_emit.append(MetadataChangeProposalWrapper(entityUrn=str(flow_urn), aspect=flow_info_aspect))

    for mcp in mcps_to_emit:
        emitter.emit(mcp)

    logging.info("Enhanced lineage processing complete.")


if __name__ == "__main__":
    main()
