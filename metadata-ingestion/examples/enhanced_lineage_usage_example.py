"""
Complete example: Using enhanced SQL lineage with your original correlated subqueries.

This example shows how the enhanced transformation logic handles your exact SQL
query with correlated subqueries, expanding the internal CTEs that sqlglot creates.
"""

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.sql_lineage_enhanced import (
    infer_lineage_from_sql_with_enhanced_transformation_logic,
)

# Your original SQL with correlated subqueries
SQL_WITH_CORRELATED_SUBQUERIES = """
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


def main():
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "  ENHANCED SQL LINEAGE - YOUR ORIGINAL SQL".center(78) + "║")
    print("║" + "  (Handles correlated subqueries automatically)".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "=" * 78 + "╝")

    # Initialize connection
    gms_endpoint = "http://localhost:9007"
    token = ""
    graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint, token=token))
    client = DataHubClient(graph=graph)

    print("\n" + "=" * 80)
    print("CREATING ENHANCED LINEAGE")
    print("=" * 80)

    print("\nThis will:")
    print("1. Parse your SQL with correlated subqueries")
    print("2. Sqlglot will internally convert them to CTEs (_u_0, _u_1, etc)")
    print("3. Enhanced logic will expand those internal CTEs")
    print("4. Result: Readable transformation logic!\n")

    try:
        # Use the enhanced version
        infer_lineage_from_sql_with_enhanced_transformation_logic(
            graph=client._graph,
            query_text=SQL_WITH_CORRELATED_SUBQUERIES,
            platform="postgres",
            default_db="ecommerce",
            default_schema="public",
            expand_ctes=True,        # Expand internal CTEs from correlated subqueries
            replace_aliases=True,    # Replace table aliases (p, ph, oi, o) with full names
            suppress_warnings=True,  # Suppress "Unknown subquery scope" warnings
        )

        print("✓ Lineage created successfully!")

        print("\n" + "=" * 80)
        print("WHAT YOU GET IN DATAHUB")
        print("=" * 80)

        print("""
Before (basic approach):
  avg_price_last_30d:
    Transformation: SQL: COALESCE("_u_0"."_col_0", "p"."base_price")
    ❌ Not helpful - what is _u_0._col_0?

After (enhanced approach):
  avg_price_last_30d:
    Transformation: SQL: COALESCE(AVG(raw_price_history.new_price), raw_products.base_price)
    ✅ Clear - shows the actual calculation!

The enhancement works because:
1. Sqlglot converts your correlated subquery to an internal CTE
2. We extract that CTE's definition
3. We expand the CTE reference in the transformation logic
4. You see the actual calculation, not the internal alias!
""")

        print("\n" + "=" * 80)
        print("COMPARISON FOR EACH COLUMN")
        print("=" * 80)

        transformations = [
            {
                "column": "product_id",
                "basic": '"p"."product_id"',
                "enhanced": '"raw_products"."product_id"',
                "note": "Simple alias replacement"
            },
            {
                "column": "avg_price_last_30d",
                "basic": 'COALESCE("_u_0"."_col_0", "p"."base_price")',
                "enhanced": 'COALESCE(AVG(raw_price_history.new_price), raw_products.base_price)',
                "note": "CTE expanded + aliases replaced"
            },
            {
                "column": "total_sold",
                "basic": 'COALESCE("_u_1"."_col_0", 0)',
                "enhanced": 'COALESCE(SUM(raw_order_items.quantity), 0)',
                "note": "CTE expanded + aliases replaced"
            },
            {
                "column": "revenue_generated",
                "basic": 'COALESCE("_u_2"."_col_0", 0)',
                "enhanced": 'COALESCE(SUM(quantity * unit_price * (1 - discount_percent/100)), 0)',
                "note": "Complex calculation expanded"
            },
            {
                "column": "profit_margin",
                "basic": 'CASE WHEN "p"."cost_price" > 0 THEN ...',
                "enhanced": 'CASE WHEN raw_products.cost_price > 0 THEN ...',
                "note": "Aliases replaced in CASE"
            },
        ]

        for t in transformations:
            print(f"\n{t['column']}:")
            print(f"  Basic:    {t['basic']}")
            print(f"  Enhanced: {t['enhanced']}")
            print(f"  Note:     {t['note']}")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return

    print("\n" + "=" * 80)
    print("KEY BENEFITS")
    print("=" * 80)
    print("""
✅ No need to rewrite your SQL!
   - Works with your original correlated subqueries
   - Sqlglot converts them to CTEs internally
   - Enhancement expands those internal CTEs

✅ Readable transformation logic
   - See actual calculations, not _u_0._col_0
   - Understand data lineage at a glance
   - Better documentation and debugging

✅ Full table names instead of aliases
   - raw_products.column instead of p.column
   - Clear which table each column comes from
   - Matches the table URNs in lineage graph

✅ Works with any SQL complexity
   - Correlated subqueries (your case)
   - CTEs (if you choose to rewrite)
   - Nested subqueries
   - Complex JOINs and aggregations
""")

    print("\n" + "=" * 80)
    print("HOW TO USE IN YOUR CODE")
    print("=" * 80)
    print("""
Replace this:
    client.lineage.infer_lineage_from_sql(
        query_text=sql,
        platform="postgres",
        default_db="ecommerce",
        default_schema="public",
    )

With this:
    from datahub.sdk.sql_lineage_enhanced import (
        infer_lineage_from_sql_with_enhanced_transformation_logic
    )

    infer_lineage_from_sql_with_enhanced_transformation_logic(
        graph=client._graph,
        query_text=sql,
        platform="postgres",
        default_db="ecommerce",
        default_schema="public",
        expand_ctes=True,        # Expand internal CTEs
        replace_aliases=True,    # Replace table aliases
    )

That's it! Your lineage will now have readable transformation logic.
""")


if __name__ == "__main__":
    main()
