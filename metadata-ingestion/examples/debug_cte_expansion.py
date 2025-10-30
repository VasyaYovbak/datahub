"""
Debug script to show detailed CTE extraction and expansion process.
Run this to see why some CTEs aren't being expanded.
"""

import logging
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.sql_lineage_enhanced import (
    infer_lineage_from_sql_with_enhanced_transformation_logic,
)

# Enable ALL logging including DEBUG
logging.basicConfig(
    level=logging.DEBUG,
    format='%(levelname)s - %(name)s - %(message)s'
)

# Make sure our module's logs are visible
logger = logging.getLogger('datahub.sdk.sql_lineage_enhanced')
logger.setLevel(logging.DEBUG)

gms_endpoint = "http://localhost:9007"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))
client = DataHubClient(graph=graph)

sql = """
INSERT INTO staging_product_metrics (
    product_id,
    avg_price_last_30d,
    total_sold
)
SELECT
    p.product_id,
    COALESCE((
        SELECT AVG(new_price)
        FROM raw_price_history ph
        WHERE ph.product_id = p.product_id
        AND ph.changed_at > CURRENT_DATE - INTERVAL '30 days'
    ), p.base_price) AS avg_price_last_30d,
    COALESCE((
        SELECT SUM(oi.quantity)
        FROM raw_order_items oi
        JOIN raw_orders o ON oi.order_id = o.order_id
        WHERE oi.product_id = p.product_id
        AND o.status IN ('shipped', 'delivered')
    ), 0) AS total_sold
FROM raw_products p;
"""

print("=" * 80)
print("RUNNING WITH DEBUG LOGGING")
print("=" * 80)
print()

infer_lineage_from_sql_with_enhanced_transformation_logic(
    graph=client._graph,
    query_text=sql,
    platform="postgres",
    default_db="ecommerce",
    default_schema="public",
    expand_ctes=True,
    replace_aliases=True,
    suppress_warnings=False,  # Don't suppress - we want to see everything!
)

print()
print("=" * 80)
print("DONE - Check the logs above to see CTE extraction and expansion details")
print("=" * 80)
