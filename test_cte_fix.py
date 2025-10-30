#!/usr/bin/env python3
"""
Quick test to verify CTE extraction now works.
"""

import sys
import logging
sys.path.insert(0, '/home/user/datahub/metadata-ingestion/src')

import sqlglot
from datahub.sdk.sql_lineage_enhanced import extract_ctes_from_optimized_sql

# Enable DEBUG logging
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(message)s')

# Simple SQL with correlated subquery
sql = """
SELECT
    p.product_id,
    COALESCE((
        SELECT AVG(new_price)
        FROM raw_price_history ph
        WHERE ph.product_id = p.product_id
    ), p.base_price) AS avg_price
FROM raw_products p
"""

print("=" * 80)
print("Testing CTE Extraction Fix")
print("=" * 80)
print()

dialect = sqlglot.Dialect.get_or_raise("postgres")
statement = sqlglot.parse_one(sql, dialect=dialect)

print("Step 1: Parse SQL")
print("✓ Parsed successfully")
print()

print("Step 2: Optimize with qualify + unnest_subqueries")
_OPTIMIZE_RULES = (
    sqlglot.optimizer.optimizer.qualify,
    sqlglot.optimizer.optimizer.unnest_subqueries,
)

try:
    optimized = sqlglot.optimizer.optimizer.optimize(
        statement.copy(),
        dialect=dialect,
        schema=None,
        qualify_columns=False,
        validate_qualify_columns=False,
        allow_partial_qualification=True,
        identify=False,
        catalog="ecommerce",
        db="public",
        rules=_OPTIMIZE_RULES,
    )
    print("✓ Optimized successfully")
    print()
    print("Optimized SQL:")
    print(optimized.sql(pretty=True, dialect=dialect))
    print()
except Exception as e:
    print(f"✗ Optimization failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("Step 3: Extract CTEs from optimized SQL")
try:
    ctes = extract_ctes_from_optimized_sql(optimized, dialect)
    print(f"✓ Extracted {len(ctes)} CTE definitions: {list(ctes.keys())}")
    print()

    if len(ctes) > 0:
        for cte_name, cte_def in ctes.items():
            print(f"CTE '{cte_name}':")
            print(f"  Columns: {list(cte_def.column_mappings.keys())}")
            for col, expr in cte_def.column_mappings.items():
                print(f"    {col} = {expr}")
            print()
        print("=" * 80)
        print("✅ SUCCESS! CTEs are now being extracted correctly!")
        print("=" * 80)
    else:
        print("=" * 80)
        print("❌ FAILED! No CTEs extracted")
        print("=" * 80)
        sys.exit(1)

except Exception as e:
    print(f"✗ CTE extraction failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
