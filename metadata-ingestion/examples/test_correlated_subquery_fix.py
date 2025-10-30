"""
Test script to evaluate potential fixes for correlated subquery lineage issues.

This script allows you to test different approaches to handling correlated subqueries
in SQL lineage parsing.
"""

import logging
from typing import Optional

import sqlglot
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.sdk.main_client import DataHubClient
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result

# Setup logging to see warnings
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# Your problematic SQL
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
    ), 0) AS total_sold,
    COALESCE((
        SELECT SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100))
        FROM raw_order_items oi
        JOIN raw_orders o ON oi.order_id = o.order_id
        WHERE oi.product_id = p.product_id
        AND o.status IN ('shipped', 'delivered')
    ), 0) AS revenue_generated,
    CASE
        WHEN p.cost_price > 0 THEN
            ((p.base_price - p.cost_price) / p.base_price * 100)
        ELSE 0
    END AS profit_margin,
    CASE
        WHEN p.stock_quantity = 0 THEN 'out_of_stock'
        WHEN p.stock_quantity < 10 THEN 'low_stock'
        ELSE 'in_stock'
    END AS stock_status,
    (
        SELECT MAX(o.order_date)
        FROM raw_order_items oi
        JOIN raw_orders o ON oi.order_id = o.order_id
        WHERE oi.product_id = p.product_id
    ) AS last_sale_date
FROM raw_products p;
"""


# Rewritten version without correlated subqueries
SQL_REWRITTEN_WITH_CTES = """
WITH price_history_avg AS (
    SELECT
        product_id,
        AVG(new_price) AS avg_price_30d
    FROM raw_price_history
    WHERE changed_at > CURRENT_DATE - INTERVAL '30 days'
    GROUP BY product_id
),
product_sales AS (
    SELECT
        oi.product_id,
        SUM(oi.quantity) AS total_quantity_sold,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)) AS total_revenue,
        MAX(o.order_date) AS last_order_date
    FROM raw_order_items oi
    JOIN raw_orders o ON oi.order_id = o.order_id
    WHERE o.status IN ('shipped', 'delivered')
    GROUP BY oi.product_id
)
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
    COALESCE(pha.avg_price_30d, p.base_price) AS avg_price_last_30d,
    COALESCE(ps.total_quantity_sold, 0) AS total_sold,
    COALESCE(ps.total_revenue, 0) AS revenue_generated,
    CASE
        WHEN p.cost_price > 0 THEN
            ((p.base_price - p.cost_price) / p.base_price * 100)
        ELSE 0
    END AS profit_margin,
    CASE
        WHEN p.stock_quantity = 0 THEN 'out_of_stock'
        WHEN p.stock_quantity < 10 THEN 'low_stock'
        ELSE 'in_stock'
    END AS stock_status,
    ps.last_order_date AS last_sale_date
FROM raw_products p
LEFT JOIN price_history_avg pha ON pha.product_id = p.product_id
LEFT JOIN product_sales ps ON ps.product_id = p.product_id;
"""


def test_sql_parsing(
    sql: str,
    title: str,
    graph: DataHubGraph,
    show_details: bool = True
):
    """
    Parse SQL and display the results.
    """
    print("\n" + "=" * 80)
    print(f"{title}")
    print("=" * 80)

    try:
        result = create_lineage_sql_parsed_result(
            query=sql,
            default_db="ecommerce",
            default_schema="public",
            platform="postgres",
            graph=graph,
        )

        print(f"\n✓ Parsing successful")
        print(f"  Output tables: {len(result.out_tables)}")
        print(f"  Input tables: {len(result.in_tables)}")
        print(f"  Column lineage relationships: {len(result.column_lineage or [])}")
        print(f"  Confidence: {result.debug_info.confidence:.2f}")

        if result.debug_info.table_error:
            print(f"  ⚠ Table error: {result.debug_info.table_error}")
        if result.debug_info.column_error:
            print(f"  ⚠ Column error: {result.debug_info.column_error}")

        if show_details and result.column_lineage:
            print(f"\nColumn Lineage Details (showing first 5):")
            for i, col_lineage in enumerate(result.column_lineage[:5], 1):
                print(f"\n{i}. {col_lineage.downstream.column}")

                upstream_cols = [
                    f"{ref.table}.{ref.column}"
                    for ref in col_lineage.upstreams
                    if ref.column
                ]
                if upstream_cols:
                    print(f"   Upstream: {', '.join(upstream_cols[:3])}")
                    if len(upstream_cols) > 3:
                        print(f"            ... and {len(upstream_cols) - 3} more")

                if col_lineage.logic:
                    logic_type = "COPY" if col_lineage.logic.is_direct_copy else "SQL"
                    logic_expr = col_lineage.logic.column_logic

                    # Truncate long expressions
                    if len(logic_expr) > 200:
                        logic_expr = logic_expr[:200] + "..."

                    print(f"   {logic_type}: {logic_expr}")

                    # Check for optimization artifacts
                    if "_u_" in logic_expr or "_col_" in logic_expr:
                        print(f"   ⚠ WARNING: Optimization artifacts detected (_u_*, _col_*)")
                else:
                    print(f"   No transformation logic")

        return result

    except Exception as e:
        print(f"\n✗ Parsing failed: {e}")
        import traceback
        traceback.print_exc()
        return None


def analyze_transformation_quality(result) -> dict:
    """
    Analyze the quality of transformation logic extraction.
    """
    if not result or not result.column_lineage:
        return {"error": "No column lineage"}

    stats = {
        "total_columns": len(result.column_lineage),
        "with_logic": 0,
        "direct_copies": 0,
        "transformations": 0,
        "with_optimization_artifacts": 0,
        "readable": 0,
    }

    for col in result.column_lineage:
        if col.logic:
            stats["with_logic"] += 1
            if col.logic.is_direct_copy:
                stats["direct_copies"] += 1
            else:
                stats["transformations"] += 1

            # Check for optimization artifacts
            if "_u_" in col.logic.column_logic or "_col_" in col.logic.column_logic:
                stats["with_optimization_artifacts"] += 1
            else:
                stats["readable"] += 1

    return stats


def print_comparison(stats1: dict, stats2: dict, label1: str, label2: str):
    """
    Print a comparison of two parsing results.
    """
    print("\n" + "=" * 80)
    print("COMPARISON")
    print("=" * 80)

    metrics = [
        ("Total columns", "total_columns"),
        ("With transformation logic", "with_logic"),
        ("Direct copies", "direct_copies"),
        ("Transformations", "transformations"),
        ("With optimization artifacts (_u_*, _col_*)", "with_optimization_artifacts"),
        ("Readable (no artifacts)", "readable"),
    ]

    print(f"\n{'Metric':<45} {label1:<20} {label2:<20}")
    print("-" * 90)

    for metric_name, metric_key in metrics:
        val1 = stats1.get(metric_key, "N/A")
        val2 = stats2.get(metric_key, "N/A")

        # Add indicator if there's a difference
        indicator = ""
        if isinstance(val1, int) and isinstance(val2, int):
            if val1 < val2:
                indicator = "✓"
            elif val1 > val2:
                indicator = "✗"

        print(f"{metric_name:<45} {str(val1):<20} {str(val2):<20} {indicator}")


def main():
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "  CORRELATED SUBQUERY LINEAGE TEST".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "=" * 78 + "╝")
    print("\n")

    # Initialize connection
    gms_endpoint = "http://localhost:9007"
    graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

    # Test 1: Original SQL with correlated subqueries
    result1 = test_sql_parsing(
        SQL_WITH_CORRELATED_SUBQUERIES,
        "TEST 1: Original SQL (with correlated subqueries)",
        graph,
        show_details=True
    )

    # Test 2: Rewritten SQL without correlated subqueries
    result2 = test_sql_parsing(
        SQL_REWRITTEN_WITH_CTES,
        "TEST 2: Rewritten SQL (with CTEs and JOINs)",
        graph,
        show_details=True
    )

    # Analyze and compare
    if result1 and result2:
        stats1 = analyze_transformation_quality(result1)
        stats2 = analyze_transformation_quality(result2)

        print_comparison(stats1, stats2, "Original", "Rewritten")

        print("\n" + "=" * 80)
        print("RECOMMENDATIONS")
        print("=" * 80)

        if stats1.get("with_optimization_artifacts", 0) > stats2.get("with_optimization_artifacts", 0):
            print("\n✓ The rewritten version produces more readable transformation logic")
            print("✓ Consider rewriting complex correlated subqueries as CTEs + JOINs")
        else:
            print("\n⚠ Both versions have similar readability")
            print("  The issue may require code-level fixes in DataHub")

        if stats2.get("readable", 0) == stats2.get("transformations", 0):
            print("✓ The rewritten version has NO optimization artifacts")
            print("✓ All transformation logic is directly readable")

    print("\n" + "=" * 80)
    print("NEXT STEPS")
    print("=" * 80)
    print("""
1. If rewritten SQL shows better results:
   → Refactor your SQL queries to use CTEs and JOINs instead of correlated subqueries

2. If both versions have issues:
   → Read CORRELATED_SUBQUERY_LINEAGE_ISSUE.md for deeper analysis
   → Consider contributing a fix to DataHub

3. For immediate use:
   → Use the transformation logic preservation function from sql_lineage_utils.py
   → Accept that complex subqueries may show optimization artifacts
   → Focus on table and column lineage accuracy over transformation readability
""")


if __name__ == "__main__":
    main()
