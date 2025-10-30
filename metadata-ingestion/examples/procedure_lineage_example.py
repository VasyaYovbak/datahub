"""
Example of using the enhanced SQL lineage processor for procedures with temp tables.

This example demonstrates:
1. Processing a PostgreSQL function with multiple operations
2. Tracking temporary tables created during execution
3. Creating DataFlow and DataJob entities in DataHub
4. Handling CTE expansion and column lineage
"""

from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.sql_lineage_enhanced import process_procedure_lineage

# Modified example procedure with temp table
PROCEDURE_WITH_TEMP_TABLE = """
CREATE OR REPLACE FUNCTION calculate_rfm_scores()
RETURNS INTEGER AS $$
DECLARE
    processed_count INTEGER;
BEGIN
    -- Clear existing scores
    TRUNCATE TABLE analytics_rfm_scores;

    -- Create temp table for customer metrics
    CREATE TEMP TABLE temp_customer_metrics AS
    SELECT
        c.customer_id,
        -- Recency: days since last order
        CURRENT_DATE - MAX(o.order_date)::DATE AS days_since_last_order,
        -- Frequency: order count
        COUNT(o.order_id) AS order_count,
        -- Monetary: total amount
        SUM(o.total_amount) AS total_spent
    FROM raw_customers c
    LEFT JOIN raw_orders o ON c.customer_id = o.customer_id
    WHERE o.status IN ('shipped', 'delivered')
    GROUP BY c.customer_id;

    -- Create temp table for RFM scores
    CREATE TEMP TABLE temp_rfm_scores AS
    SELECT
        customer_id,
        -- Recency score (fewer days = higher score)
        CASE
            WHEN days_since_last_order <= 30 THEN 5
            WHEN days_since_last_order <= 60 THEN 4
            WHEN days_since_last_order <= 90 THEN 3
            WHEN days_since_last_order <= 180 THEN 2
            ELSE 1
        END AS recency_score,
        -- Frequency score
        CASE
            WHEN order_count >= 20 THEN 5
            WHEN order_count >= 10 THEN 4
            WHEN order_count >= 5 THEN 3
            WHEN order_count >= 2 THEN 2
            ELSE 1
        END AS frequency_score,
        -- Monetary score
        CASE
            WHEN total_spent >= 5000 THEN 5
            WHEN total_spent >= 2000 THEN 4
            WHEN total_spent >= 1000 THEN 3
            WHEN total_spent >= 500 THEN 2
            ELSE 1
        END AS monetary_score
    FROM temp_customer_metrics;

    -- Insert final scores using temp table
    INSERT INTO analytics_rfm_scores (
        customer_id,
        recency_score,
        frequency_score,
        monetary_score,
        rfm_segment
    )
    SELECT
        customer_id,
        recency_score,
        frequency_score,
        monetary_score,
        CASE
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
            WHEN recency_score >= 3 AND frequency_score >= 3 THEN 'Loyal Customers'
            WHEN recency_score >= 4 AND frequency_score <= 2 THEN 'Promising'
            WHEN recency_score <= 2 AND frequency_score >= 3 THEN 'At Risk'
            WHEN recency_score <= 2 AND frequency_score <= 2 THEN 'Lost'
            ELSE 'Regular'
        END AS rfm_segment
    FROM temp_rfm_scores;

    -- Clear staging table
    TRUNCATE TABLE staging_product_metrics;

    -- Populate product metrics
    INSERT INTO staging_product_metrics (
        product_id,
        product_name,
        category,
        current_price,
        avg_price_last_30d,
        total_sold,
        revenue_generated,
        profit_margin,
        stock_status,
        last_sale_date
    )
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        p.base_price AS current_price,
        -- Average price last 30 days
        COALESCE((
            SELECT AVG(new_price)
            FROM raw_price_history ph
            WHERE ph.product_id = p.product_id
            AND ph.changed_at > CURRENT_DATE - INTERVAL '30 days'
        ), p.base_price) AS avg_price_last_30d,
        -- Total sold quantity
        COALESCE((
            SELECT SUM(oi.quantity)
            FROM raw_order_items oi
            JOIN raw_orders o ON oi.order_id = o.order_id
            WHERE oi.product_id = p.product_id
            AND o.status IN ('shipped', 'delivered')
        ), 0) AS total_sold,
        -- Revenue
        COALESCE((
            SELECT SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100))
            FROM raw_order_items oi
            JOIN raw_orders o ON oi.order_id = o.order_id
            WHERE oi.product_id = p.product_id
            AND o.status IN ('shipped', 'delivered')
        ), 0) AS revenue_generated,
        -- Profit margin
        CASE
            WHEN p.cost_price > 0 THEN
                ((p.base_price - p.cost_price) / p.base_price * 100)
            ELSE 0
        END AS profit_margin,
        -- Stock status
        CASE
            WHEN p.stock_quantity = 0 THEN 'out_of_stock'
            WHEN p.stock_quantity < 10 THEN 'low_stock'
            ELSE 'in_stock'
        END AS stock_status,
        -- Last sale date
        (
            SELECT MAX(o.order_date)
            FROM raw_order_items oi
            JOIN raw_orders o ON oi.order_id = o.order_id
            WHERE oi.product_id = p.product_id
        ) AS last_sale_date
    FROM raw_products p;

    GET DIAGNOSTICS processed_count = ROW_COUNT;
    RETURN processed_count;
END;
$$ LANGUAGE plpgsql;
"""


def main():
    """Main example function."""
    # Initialize DataHub connection
    # Replace with your actual DataHub GMS endpoint
    graph = DataHubGraph(config={"server": "http://localhost:8080"})

    # Process the procedure lineage
    print("🚀 Starting procedure lineage processing...")

    process_procedure_lineage(
        graph=graph,
        procedure_sql=PROCEDURE_WITH_TEMP_TABLE,
        procedure_name="calculate_rfm_scores",
        platform="postgres",
        env="PROD",
        default_db="analytics_db",
        default_schema="public",
        expand_ctes=True,
        replace_aliases=True,
        suppress_warnings=True,
    )

    print("\n✅ Procedure lineage processing complete!")
    print("\nExpected structure in DataHub:")
    print("📊 DataFlow: calculate_rfm_scores")
    print("   ├── 📝 DataJob: calculate_rfm_scores_start")
    print("   ├── 🗑️  DataJob: calculate_rfm_scores_node_1 (TRUNCATE analytics_rfm_scores)")
    print("   ├── 📋 DataJob: calculate_rfm_scores_node_2 (CREATE TEMP TABLE temp_customer_metrics)")
    print("   ├── 📋 DataJob: calculate_rfm_scores_node_3 (CREATE TEMP TABLE temp_rfm_scores)")
    print("   ├── ➕ DataJob: calculate_rfm_scores_node_4 (INSERT into analytics_rfm_scores)")
    print("   ├── 🗑️  DataJob: calculate_rfm_scores_node_5 (TRUNCATE staging_product_metrics)")
    print("   └── ➕ DataJob: calculate_rfm_scores_node_6 (INSERT into staging_product_metrics)")
    print("\n📌 Temp tables tracked:")
    print("   - temp_customer_metrics (with column lineage)")
    print("   - temp_rfm_scores (with column lineage from temp_customer_metrics)")


if __name__ == "__main__":
    main()
