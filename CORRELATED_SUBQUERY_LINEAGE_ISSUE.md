# Investigation: "Unknown Subquery Scope" Warnings and Incorrect Transformation Logic

## Problem Summary

When processing SQL queries with **correlated subqueries** (subqueries that reference columns from the outer query), DataHub's SQL parser generates:

1. **Warnings:** `Unknown subquery scope: SELECT ...`
2. **Unreadable transformation logic:** Shows `_u_0._col_0` instead of the original subquery expression
3. **Potentially incorrect lineage:** In some cases, the correlation context is lost

## Root Cause Analysis

### File: `metadata-ingestion/src/datahub/sql_parsing/sqlglot_lineage.py`

### The Optimization Pipeline

**Lines 1395-1407:**
```python
original_statement, statement = statement, statement.copy()
# ...
statement = sqlglot.optimizer.qualify.qualify(statement, ...)
```

DataHub preserves the original statement but then works only with the optimized copy.

**Lines 459-471 (`_prepare_query_columns` function):**
```python
statement = sqlglot.optimizer.optimizer.optimize(
    statement,
    dialect=dialect,
    schema=sqlglot_db_schema,
    # ... other options ...
    rules=_OPTIMIZE_RULES,
)
```

**Lines 89-106 (`_OPTIMIZE_RULES` definition):**
```python
_OPTIMIZE_RULES = (
    sqlglot.optimizer.optimizer.qualify,
    sqlglot.optimizer.optimizer.pushdown_projections,
    sqlglot.optimizer.optimizer.unnest_subqueries,  # ← THIS IS THE CULPRIT!
    sqlglot.optimizer.optimizer.quote_identifiers,
)
```

### What `unnest_subqueries` Does

The `unnest_subqueries` optimization **transforms correlated subqueries into JOINs** with derived tables (inline views).

**Your original SQL:**
```sql
COALESCE((
    SELECT AVG(new_price)
    FROM raw_price_history ph
    WHERE ph.product_id = p.product_id  -- Correlated!
    AND ph.changed_at > CURRENT_DATE - INTERVAL '30 days'
), p.base_price) AS avg_price_last_30d
```

**After `unnest_subqueries` optimization:**
```sql
COALESCE("_u_0"."_col_0", "p"."base_price") AS "avg_price_last_30d"
```

Where `_u_0` is a derived table (CTE) created by the optimizer:
```sql
SELECT AVG(ph.new_price) AS _col_0, ph.product_id AS _u_1
FROM raw_price_history ph
WHERE ph.changed_at > CURRENT_DATE - INTERVAL '30 days'
GROUP BY ph.product_id
```

### Why This Optimization Exists

Sqlglot applies this optimization because:
1. **Lineage computation is easier through JOINs** than through nested correlated subqueries
2. **Column dependencies are more explicit** when subqueries are flattened
3. **Type inference works better** on flat query structures

### The "Unknown Subquery Scope" Warnings

These warnings occur when sqlglot's optimizer encounters correlated subqueries that it **cannot fully unnest** or where the correlation context is **ambiguous**.

Possible causes:
- Multiple levels of nesting
- Complex correlation predicates
- Aggregate functions with multiple tables in FROM clause
- Outer references that aren't clear to the optimizer

## Impact on Your Use Case

### What Works Correctly
✅ Table-level lineage (which tables feed which tables)
✅ Column-level dependencies (which columns are used in the calculation)
✅ The lineage graph structure

### What Breaks
❌ **Readability:** Transformation logic shows `_u_0._col_0` instead of the actual subquery
❌ **Understanding:** Users can't see the actual calculation logic
❌ **Potentially wrong lineage:** If unnesting fails, some column dependencies might be missed

### Example from Your Output

```
5. avg_price_last_30d
   Upstream columns:
     - raw_price_history.new_price
     - raw_products.base_price
   Transformation: SQL: COALESCE("_u_0"."_col_0", "p"."base_price")
```

**Expected transformation logic:**
```sql
COALESCE((
    SELECT AVG(new_price)
    FROM raw_price_history ph
    WHERE ph.product_id = p.product_id
    AND ph.changed_at > CURRENT_DATE - INTERVAL '30 days'
), p.base_price)
```

## Potential Solutions

### Option 1: Extract Transformation Logic from Original Statement ⭐ RECOMMENDED

**Approach:**
- Use optimized statement for lineage computation (correct dependencies)
- Use original statement for transformation logic extraction (readable expressions)

**Implementation:**
1. In `_sqlglot_lineage_inner()`, pass both `original_statement` and `statement` to `_column_level_lineage()`
2. In `_select_statement_cll()`, extract column expressions from the original SELECT before optimization
3. Store these original expressions alongside the optimized lineage nodes
4. In `_get_column_transformation()`, use the original expression if available

**Pros:**
- Best of both worlds: correct lineage + readable transformations
- No loss of functionality
- Backward compatible

**Cons:**
- More complex implementation
- Need to map optimized column aliases back to original expressions

### Option 2: Disable `unnest_subqueries` Optimization

**Approach:**
Remove `sqlglot.optimizer.optimizer.unnest_subqueries` from `_OPTIMIZE_RULES`.

**Implementation:**
```python
_OPTIMIZE_RULES = (
    sqlglot.optimizer.optimizer.qualify,
    sqlglot.optimizer.optimizer.pushdown_projections,
    # sqlglot.optimizer.optimizer.unnest_subqueries,  # DISABLED
    sqlglot.optimizer.optimizer.quote_identifiers,
)
```

**Pros:**
- Simple one-line change
- Preserves original SQL structure
- No "Unknown subquery scope" warnings

**Cons:**
- **May break lineage computation for complex queries**
- Sqlglot's lineage algorithm might not handle correlated subqueries well
- Could cause other optimization issues
- **RISKY:** Not tested extensively

### Option 3: Make Unnesting Conditional

**Approach:**
Add a parameter to control whether to unnest subqueries.

**Implementation:**
```python
def _prepare_query_columns(
    statement: sqlglot.exp.Expression,
    dialect: sqlglot.Dialect,
    table_schemas: Dict[_TableName, SchemaInfo],
    default_db: Optional[str],
    default_schema: Optional[str],
    unnest_subqueries: bool = True,  # NEW PARAMETER
) -> Tuple[sqlglot.exp.Expression, "_ColumnResolver"]:
    rules = list(_OPTIMIZE_RULES)
    if not unnest_subqueries:
        rules = [r for r in rules if r != sqlglot.optimizer.optimizer.unnest_subqueries]

    statement = sqlglot.optimizer.optimizer.optimize(
        statement,
        dialect=dialect,
        schema=sqlglot_db_schema,
        rules=tuple(rules),
        # ...
    )
```

**Pros:**
- Flexible: users can choose based on their needs
- Doesn't break existing functionality

**Cons:**
- Adds API complexity
- Still requires testing both paths
- Users need to understand when to use which option

### Option 4: Use Sqlglot's Lineage API Directly (Experimental)

**Approach:**
Instead of using the optimized statement, use sqlglot's lineage API on the original statement.

**Implementation:**
```python
from sqlglot import lineage

# Get lineage directly from sqlglot
lineage_result = lineage.lineage(
    column="avg_price_last_30d",
    sql=original_statement,
    dialect=dialect,
)
```

**Pros:**
- Uses sqlglot's native lineage computation
- May handle subqueries better

**Cons:**
- Requires significant refactoring
- Sqlglot's lineage API is still evolving
- May not integrate well with DataHub's schema resolution

## Recommended Solution

**Implement Option 1** (Extract transformation logic from original statement) as a **future enhancement**, but provide an **immediate workaround** for users.

### Immediate Workaround

Users can work around this issue by:

1. **Rewriting queries to avoid correlated subqueries**
   - Use JOINs instead of subqueries where possible
   - Pre-compute aggregates in CTEs

2. **Accept the current behavior for complex queries**
   - The lineage relationships are still correct
   - The transformation logic is less readable but not incorrect

3. **Custom transformation logic annotation**
   - Manually add comments or metadata to describe transformations

### Example: Rewriting to Avoid Correlated Subqueries

**Original (with correlated subquery):**
```sql
SELECT
    p.product_id,
    COALESCE((
        SELECT AVG(new_price)
        FROM raw_price_history ph
        WHERE ph.product_id = p.product_id
        AND ph.changed_at > CURRENT_DATE - INTERVAL '30 days'
    ), p.base_price) AS avg_price_last_30d
FROM raw_products p
```

**Rewritten (with LEFT JOIN):**
```sql
WITH price_avg AS (
    SELECT
        product_id,
        AVG(new_price) AS avg_30d
    FROM raw_price_history
    WHERE changed_at > CURRENT_DATE - INTERVAL '30 days'
    GROUP BY product_id
)
SELECT
    p.product_id,
    COALESCE(pa.avg_30d, p.base_price) AS avg_price_last_30d
FROM raw_products p
LEFT JOIN price_avg pa ON pa.product_id = p.product_id
```

**Benefits of rewrite:**
- ✅ No "Unknown subquery scope" warnings
- ✅ Clearer transformation logic in DataHub
- ✅ Better query performance (in most databases)
- ✅ More explicit column dependencies

## Testing the Issue

### Reproduce the Warnings

```python
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result

graph = DataHubGraph(DatahubClientConfig(server="http://localhost:9007"))

sql_with_correlated = """
SELECT
    p.product_id,
    COALESCE((
        SELECT SUM(oi.quantity)
        FROM raw_order_items oi
        WHERE oi.product_id = p.product_id
    ), 0) AS total_sold
FROM raw_products p
"""

# This will generate warnings
result = create_lineage_sql_parsed_result(
    query=sql_with_correlated,
    default_db="ecommerce",
    default_schema="public",
    platform="postgres",
    graph=graph,
)

for col in result.column_lineage:
    print(f"{col.downstream.column}: {col.logic.column_logic if col.logic else 'N/A'}")
```

### Expected Output (Current Behavior)

```
Unknown subquery scope: SELECT "oi"."product_id" AS "product_id", "oi"."quantity" AS "quantity" FROM "ecommerce"."public"."raw_order_items" AS "oi"

total_sold: COALESCE("_u_0"."_col_0", 0)
```

### Desired Output (Future)

```
total_sold: COALESCE((SELECT SUM(oi.quantity) FROM raw_order_items oi WHERE oi.product_id = p.product_id), 0)
```

## Impact Assessment

### Low Impact Queries (Work Fine)
- Simple SELECT with JOINs
- Queries without subqueries
- CTEs without correlation
- Window functions
- Simple aggregations

### High Impact Queries (Problematic)
- Correlated scalar subqueries in SELECT clause
- Multiple nested correlated subqueries
- Correlated EXISTS/IN clauses in WHERE
- Aggregate subqueries with correlation

### Your Specific Query

Your query has **high impact** because it contains:
- ✅ 6 correlated subqueries in SELECT clause
- ✅ Multiple levels of JOINs within subqueries
- ✅ Aggregate functions (AVG, SUM, MAX) in subqueries
- ✅ Complex WHERE clauses with correlation

## Next Steps

### Short Term (Immediate)

1. **Document the limitation** in your codebase
2. **Consider query rewrites** for critical lineage tracking
3. **Accept less readable transformation logic** for complex queries
4. **Use table-level lineage** as the primary trust source

### Medium Term (If Needed)

1. **File an issue** with DataHub project about this limitation
2. **Test Option 2** (disabling unnest_subqueries) in a development environment
3. **Monitor** if lineage accuracy is affected

### Long Term (Contribution)

1. **Implement Option 1** (extract from original statement)
2. **Submit PR** to DataHub with comprehensive tests
3. **Add configuration option** for unnesting behavior

## References

- DataHub SQL Parsing Code: `metadata-ingestion/src/datahub/sql_parsing/sqlglot_lineage.py`
- Sqlglot Optimizer Documentation: https://sqlglot.com/sqlglot/optimizer/optimizer.html
- Sqlglot Unnest Subqueries: https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/unnest_subqueries.py
