"""
Example: Using SQL lineage with transformation logic preservation.

This example demonstrates the difference between the standard infer_lineage_from_sql
and the new infer_lineage_from_sql_with_transformation_logic function.

The new function preserves the SQL transformation logic for each column, so you can
see exactly how each output column is calculated from input columns.
"""

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.sql_lineage_utils import (
    infer_lineage_from_sql_with_transformation_logic,
)

# Configuration
gms_endpoint = "http://localhost:9007"
token = ""

# Initialize clients
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint, token=token))
client = DataHubClient(graph=graph)

# Complex SQL with transformations
sql = """
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
    -- Average price over last 30 days
    COALESCE((
        SELECT AVG(new_price)
        FROM raw_price_history ph
        WHERE ph.product_id = p.product_id
        AND ph.changed_at > CURRENT_DATE - INTERVAL '30 days'
    ), p.base_price) AS avg_price_last_30d,
    -- Total quantity sold
    COALESCE((
        SELECT SUM(oi.quantity)
        FROM raw_order_items oi
        JOIN raw_orders o ON oi.order_id = o.order_id
        WHERE oi.product_id = p.product_id
        AND o.status IN ('shipped', 'delivered')
    ), 0) AS total_sold,
    -- Revenue generated
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
"""


# ==============================================================================
# Method 1: Standard approach (loses transformation logic)
# ==============================================================================
def example_standard_approach():
    """
    The standard approach creates lineage but loses the transformation logic.

    Result:
    - You see that product_id → product_id
    - You see that total_sold depends on oi.quantity
    - But you DON'T see the actual SQL: COALESCE(SUM(oi.quantity), 0)
    """
    print("=" * 80)
    print("STANDARD APPROACH (loses transformation logic)")
    print("=" * 80)

    client.lineage.infer_lineage_from_sql(
        query_text=sql,
        platform="postgres",
        default_db="ecommerce",
        default_schema="public",
    )

    print("✓ Lineage created with table and column mappings")
    print("✗ Transformation logic (SQL expressions) NOT preserved")
    print()


# ==============================================================================
# Method 2: Enhanced approach (preserves transformation logic)
# ==============================================================================
def example_enhanced_approach():
    """
    The enhanced approach preserves the full transformation logic.

    Result:
    - You see that product_id → product_id with "COPY: p.product_id"
    - You see that total_sold with "SQL: COALESCE((SELECT SUM(oi.quantity)...))"
    - You can see EXACTLY how each column is calculated!
    """
    print("=" * 80)
    print("ENHANCED APPROACH (preserves transformation logic)")
    print("=" * 80)

    infer_lineage_from_sql_with_transformation_logic(
        graph=client._graph,  # Can also pass client directly
        query_text=sql,
        platform="postgres",
        default_db="ecommerce",
        default_schema="public",
    )

    print("✓ Lineage created with table and column mappings")
    print("✓ Transformation logic (SQL expressions) PRESERVED")
    print("✓ You can now see in DataHub UI how each column is calculated")
    print()


# ==============================================================================
# Method 3: Using parsed result directly for inspection
# ==============================================================================
def example_inspect_parsed_result():
    """
    You can also parse the SQL and inspect the results before emitting lineage.
    """
    print("=" * 80)
    print("INSPECT PARSED RESULT (before emitting)")
    print("=" * 80)

    from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result

    parsed_result = create_lineage_sql_parsed_result(
        query=sql,
        default_db="ecommerce",
        default_schema="public",
        platform="postgres",
        graph=client._graph,
    )

    print(f"Output tables: {parsed_result.out_tables}")
    print(f"Input tables: {parsed_result.in_tables}")
    print(f"\nColumn lineage ({len(parsed_result.column_lineage or [])} relationships):")
    print()

    if parsed_result.column_lineage:
        for i, col_lineage in enumerate(parsed_result.column_lineage[:5], 1):  # Show first 5
            downstream = col_lineage.downstream.column
            upstreams = [f"{ref.table}.{ref.column}" for ref in col_lineage.upstreams]

            print(f"{i}. {downstream}")
            print(f"   Upstream columns: {', '.join(upstreams) if upstreams else 'None'}")

            if col_lineage.logic:
                logic_type = "COPY" if col_lineage.logic.is_direct_copy else "SQL"
                logic_expr = col_lineage.logic.column_logic[:100]  # Truncate long expressions
                print(f"   Transformation: {logic_type}: {logic_expr}...")
            else:
                print(f"   Transformation: None")
            print()

    # Now you can emit it with the enhanced function
    infer_lineage_from_sql_with_transformation_logic(
        graph=client._graph,
        query_text=sql,
        platform="postgres",
        default_db="ecommerce",
        default_schema="public",
    )


# ==============================================================================
# Main execution
# ==============================================================================
if __name__ == "__main__":
    import sys

    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "  SQL LINEAGE WITH TRANSFORMATION LOGIC - EXAMPLES".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "=" * 78 + "╝")
    print("\n")

    try:
        # You can run individual examples or all of them
        if len(sys.argv) > 1:
            example_choice = sys.argv[1]
            if example_choice == "standard":
                example_standard_approach()
            elif example_choice == "enhanced":
                example_enhanced_approach()
            elif example_choice == "inspect":
                example_inspect_parsed_result()
            else:
                print(f"Unknown example: {example_choice}")
                print("Usage: python example.py [standard|enhanced|inspect]")
        else:
            # Run all examples
            example_standard_approach()
            example_enhanced_approach()
            example_inspect_parsed_result()

        print("\n✅ Examples completed successfully!\n")

    except Exception as e:
        print(f"\n❌ Error: {e}\n")
        import traceback
        traceback.print_exc()


# ==============================================================================
# Expected Results in DataHub UI
# ==============================================================================
"""
When you view the lineage in DataHub UI for staging_product_metrics:

BEFORE (standard approach):
├─ product_id
│  └─ Upstream: raw_products.product_id
│     Transformation: [Full SQL query shown as text]
│
├─ total_sold
│  └─ Upstream: raw_order_items.quantity
│     Transformation: [Full SQL query shown as text]

AFTER (enhanced approach):
├─ product_id
│  └─ Upstream: raw_products.product_id
│     Transformation: COPY: p.product_id
│
├─ total_sold
│  └─ Upstream: raw_order_items.quantity, raw_orders.status
│     Transformation: SQL: COALESCE((SELECT SUM(oi.quantity) FROM raw_order_items oi
│                                    JOIN raw_orders o ON oi.order_id = o.order_id
│                                    WHERE oi.product_id = p.product_id
│                                    AND o.status IN ('shipped', 'delivered')), 0)
│
├─ profit_margin
│  └─ Upstream: raw_products.cost_price, raw_products.base_price
│     Transformation: SQL: CASE WHEN p.cost_price > 0 THEN
│                          ((p.base_price - p.cost_price) / p.base_price * 100) ELSE 0 END

Now you can see EXACTLY how each column is derived! 🎉
"""
