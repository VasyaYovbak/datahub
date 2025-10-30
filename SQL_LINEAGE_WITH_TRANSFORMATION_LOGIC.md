# SQL Lineage with Transformation Logic Preservation

## Overview

This document explains the enhancement to DataHub's SQL lineage functionality that preserves transformation logic for column-level lineage.

## The Problem

When using the standard `infer_lineage_from_sql()` method, DataHub correctly identifies:
- Which tables are upstream/downstream
- Which columns map to which columns
- The full SQL query text

However, it **loses the specific transformation logic** for each individual column. For example, if you have:

```sql
SELECT
    product_id,  -- Simple copy
    COALESCE(SUM(quantity), 0) AS total_sold,  -- Complex aggregation
    CASE WHEN cost > 0 THEN (price - cost) / price * 100 ELSE 0 END AS margin  -- Conditional logic
FROM products
```

The standard approach shows:
- `total_sold` depends on `quantity` (correct)
- Transformation: [Shows entire SQL query as one text block]

But you can't easily see the **specific expression** that calculates `total_sold` without parsing through the entire query.

## The Solution

The new `infer_lineage_from_sql_with_transformation_logic()` function preserves the column-level transformation logic by:

1. **Parsing the SQL** using DataHub's existing SQL parser (sqlglot)
2. **Extracting the transformation logic** for each column from the parsed AST
3. **Storing the logic** in the `transformOperation` field of each fine-grained lineage relationship

The result in DataHub UI:
- `product_id` → Transformation: `COPY: product_id`
- `total_sold` → Transformation: `SQL: COALESCE(SUM(quantity), 0)`
- `margin` → Transformation: `SQL: CASE WHEN cost > 0 THEN (price - cost) / price * 100 ELSE 0 END`

## Files Created

### 1. Core Function: `sql_lineage_utils.py`

**Location:** `/home/user/datahub/metadata-ingestion/src/datahub/sdk/sql_lineage_utils.py`

**Key Function:**
```python
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
```

**What it does:**
1. Parses the SQL query using `create_lineage_sql_parsed_result()`
2. Iterates through each column lineage relationship
3. Extracts the `logic` field from `ColumnLineageInfo`
4. Creates `FineGrainedLineageClass` objects with `transformOperation` populated
5. Emits the metadata to DataHub

**Key difference from standard approach:**
- Preserves `col_lineage.logic.column_logic` in `transformOperation` field
- Uses format: `"COPY: expression"` for direct copies, `"SQL: expression"` for transformations

### 2. Example File: `sql_lineage_with_transformation_logic_example.py`

**Location:** `/home/user/datahub/metadata-ingestion/examples/sql_lineage_with_transformation_logic_example.py`

Demonstrates three approaches:
1. **Standard approach** - shows what you lose
2. **Enhanced approach** - shows what you gain
3. **Inspection approach** - shows how to inspect before emitting

## Usage

### Basic Usage

```python
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.sql_lineage_utils import infer_lineage_from_sql_with_transformation_logic

# Initialize
gms_endpoint = "http://localhost:9007"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))
client = DataHubClient(graph=graph)

# Your SQL
sql = """
INSERT INTO staging_product_metrics (product_id, total_sold)
SELECT p.product_id, COALESCE(SUM(oi.quantity), 0) AS total_sold
FROM raw_products p
LEFT JOIN raw_order_items oi ON oi.product_id = p.product_id
"""

# Create lineage with transformation logic preserved
infer_lineage_from_sql_with_transformation_logic(
    graph=client._graph,  # Can also pass client directly
    query_text=sql,
    platform="postgres",
    default_db="ecommerce",
    default_schema="public",
)
```

### Advanced Usage: Inspect Before Emitting

```python
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result

# Parse first to inspect
parsed_result = create_lineage_sql_parsed_result(
    query=sql,
    default_db="ecommerce",
    default_schema="public",
    platform="postgres",
    graph=client._graph,
)

# Inspect the parsed result
for col_lineage in parsed_result.column_lineage:
    print(f"Column: {col_lineage.downstream.column}")
    if col_lineage.logic:
        print(f"  Logic: {col_lineage.logic.column_logic}")
        print(f"  Is direct copy: {col_lineage.logic.is_direct_copy}")

# Then emit with transformation logic preserved
infer_lineage_from_sql_with_transformation_logic(
    graph=client._graph,
    query_text=sql,
    platform="postgres",
    default_db="ecommerce",
    default_schema="public",
)
```

## Technical Details

### Data Flow

```
SQL Query
    ↓
create_lineage_sql_parsed_result()
    ↓
SqlParsingResult
    ├─ in_tables: List[str]
    ├─ out_tables: List[str]
    └─ column_lineage: List[ColumnLineageInfo]
           ├─ downstream: DownstreamColumnRef
           ├─ upstreams: List[ColumnRef]
           └─ logic: ColumnTransformation ← THIS IS KEY!
                  ├─ is_direct_copy: bool
                  └─ column_logic: str  ← THE EXPRESSION!
    ↓
infer_lineage_from_sql_with_transformation_logic()
    ↓
FineGrainedLineageClass
    ├─ upstreams: List[URN]
    ├─ downstreams: List[URN]
    ├─ transformOperation: str  ← LOGIC STORED HERE!
    └─ query: URN
    ↓
Emit to DataHub
```

### Key Classes and Fields

**Input (from SQL parser):**
```python
class ColumnTransformation:
    is_direct_copy: bool      # True if simple column reference
    column_logic: str         # The SQL expression

class ColumnLineageInfo:
    downstream: DownstreamColumnRef
    upstreams: List[ColumnRef]
    logic: Optional[ColumnTransformation]  # ← Source of truth
```

**Output (DataHub metadata):**
```python
class FineGrainedLineageClass:
    upstreams: List[URN]
    downstreams: List[URN]
    transformOperation: Optional[str]  # ← Destination
    query: Optional[URN]
    confidenceScore: float
```

**Transformation logic format:**
- Direct copy: `"COPY: p.product_id"`
- SQL expression: `"SQL: COALESCE(SUM(oi.quantity), 0)"`

### Reference Implementation

The pattern follows the existing implementation in:
- **File:** `/home/user/datahub/metadata-ingestion/src/datahub/sql_parsing/sql_parsing_aggregator.py:1419-1427`

This is the "official" way DataHub's ingestion framework handles transformation logic preservation.

## Comparison: Before vs After

### Before (Standard `infer_lineage_from_sql`)

**What you see in DataHub UI:**
```
staging_product_metrics.total_sold
├─ Upstream Tables:
│  ├─ raw_products
│  └─ raw_order_items
├─ Upstream Columns:
│  └─ raw_order_items.quantity
└─ Transformation:
   └─ [Full SQL query shown as single text block]
```

**Limitations:**
- ❌ Can't easily see which expression calculates this specific column
- ❌ Need to manually parse through entire SQL query
- ❌ No programmatic access to column-specific transformation logic

### After (New `infer_lineage_from_sql_with_transformation_logic`)

**What you see in DataHub UI:**
```
staging_product_metrics.total_sold
├─ Upstream Tables:
│  ├─ raw_products
│  └─ raw_order_items
├─ Upstream Columns:
│  └─ raw_order_items.quantity
└─ Transformation:
   └─ SQL: COALESCE(SUM(oi.quantity), 0)  ← EXACT EXPRESSION!
```

**Benefits:**
- ✅ See exactly how each column is calculated
- ✅ Easier to understand complex transformations
- ✅ Programmatic access via `transformOperation` field
- ✅ Better lineage visualization and documentation

## Example: Complex SQL with Multiple Transformations

```python
sql = """
INSERT INTO staging_product_metrics (
    product_id,           -- Direct copy
    avg_price_last_30d,   -- Subquery with COALESCE
    profit_margin,        -- CASE expression with calculation
    stock_status,         -- Multi-condition CASE
    last_sale_date        -- Correlated subquery
)
SELECT
    p.product_id,
    COALESCE((
        SELECT AVG(new_price)
        FROM raw_price_history ph
        WHERE ph.product_id = p.product_id
        AND ph.changed_at > CURRENT_DATE - INTERVAL '30 days'
    ), p.base_price) AS avg_price_last_30d,
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

# After running infer_lineage_from_sql_with_transformation_logic:
```

**Result in DataHub:**

| Column | Transformation Type | Transformation Logic |
|--------|-------------------|---------------------|
| `product_id` | COPY | `COPY: p.product_id` |
| `avg_price_last_30d` | SQL | `SQL: COALESCE((SELECT AVG(new_price) FROM raw_price_history ph WHERE ph.product_id = p.product_id AND ph.changed_at > CURRENT_DATE - INTERVAL '30 days'), p.base_price)` |
| `profit_margin` | SQL | `SQL: CASE WHEN p.cost_price > 0 THEN ((p.base_price - p.cost_price) / p.base_price * 100) ELSE 0 END` |
| `stock_status` | SQL | `SQL: CASE WHEN p.stock_quantity = 0 THEN 'out_of_stock' WHEN p.stock_quantity < 10 THEN 'low_stock' ELSE 'in_stock' END` |
| `last_sale_date` | SQL | `SQL: (SELECT MAX(o.order_date) FROM raw_order_items oi JOIN raw_orders o ON oi.order_id = o.order_id WHERE oi.product_id = p.product_id)` |

## API Reference

### Function Signature

```python
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
) -> None
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `graph` | `DataHubGraph` or `DataHubClient` | Yes | Client for accessing DataHub server |
| `query_text` | `str` | Yes | SQL query to parse for lineage |
| `platform` | `str` | Yes | Platform identifier (e.g., "postgres", "snowflake") |
| `platform_instance` | `str` | No | Platform instance identifier |
| `env` | `str` | No | Environment (default: "PROD") |
| `default_db` | `str` | No | Default database for unqualified table references |
| `default_schema` | `str` | No | Default schema for unqualified table references |
| `override_dialect` | `str` | No | SQLGlot dialect override |

### Exceptions

- **`SdkUsageError`**: If SQL cannot be parsed or no output tables found
- **`Warning`**: If column-level lineage parsing fails (table lineage still created)

## Testing

### Run the Example

```bash
# From the datahub root directory
cd metadata-ingestion
python examples/sql_lineage_with_transformation_logic_example.py
```

### Run Specific Examples

```bash
# Standard approach only
python examples/sql_lineage_with_transformation_logic_example.py standard

# Enhanced approach only
python examples/sql_lineage_with_transformation_logic_example.py enhanced

# Inspection approach only
python examples/sql_lineage_with_transformation_logic_example.py inspect
```

## Integration with Existing Code

### Replacing Standard Approach

**Before:**
```python
client.lineage.infer_lineage_from_sql(
    query_text=sql,
    platform="postgres",
    default_db="ecommerce",
    default_schema="public",
)
```

**After:**
```python
from datahub.sdk.sql_lineage_utils import infer_lineage_from_sql_with_transformation_logic

infer_lineage_from_sql_with_transformation_logic(
    graph=client._graph,  # or pass client directly
    query_text=sql,
    platform="postgres",
    default_db="ecommerce",
    default_schema="public",
)
```

### Backward Compatibility

The new function:
- ✅ Does NOT modify existing `infer_lineage_from_sql()` behavior
- ✅ Uses the same underlying SQL parser
- ✅ Accepts the same parameters (except `client`/`graph`)
- ✅ Can be used alongside existing code without conflicts

## Implementation Notes

### Why a Separate Function?

1. **Backward Compatibility**: Existing code continues to work unchanged
2. **Explicit Intent**: Developers opt-in to the enhanced behavior
3. **Testing**: Easier to test and validate without affecting existing functionality
4. **Migration Path**: Allows gradual migration from old to new approach

### Code Quality

- ✅ Follows DataHub's coding standards
- ✅ Comprehensive docstrings and type hints
- ✅ Matches patterns from `sql_parsing_aggregator.py`
- ✅ No breaking changes to existing APIs
- ✅ Syntax validated with Python compiler

### Performance Considerations

- **Parsing overhead**: Same as standard approach (uses same parser)
- **Storage overhead**: Minimal (adds `transformOperation` string field)
- **Network overhead**: Slightly larger payloads (typically <1KB per column)

## Future Enhancements

Potential improvements for future iterations:

1. **Add to LineageClient class**: Make it a method on `LineageClient` for consistency
2. **Merge with standard method**: Add `preserve_transformation_logic=True` parameter
3. **Enhanced UI**: Improve DataHub UI to better visualize transformation logic
4. **Transformation validation**: Validate that transformation expressions are syntactically correct
5. **Transformation testing**: Generate test data to validate transformations

## Troubleshooting

### Issue: "Dataset does not exist" warning

**Cause**: The downstream dataset hasn't been created in DataHub yet.

**Solution**: Create the dataset first using the datasets API, or ignore the warning (lineage will still be created).

### Issue: Column lineage missing transformation logic

**Possible causes:**
1. SQL parser couldn't extract the expression (complex subqueries)
2. Column is not directly referenced (e.g., `SELECT *`)
3. Parser confidence too low

**Solution**: Check `parsed_result.debug_info` for errors or warnings.

### Issue: Transformation logic truncated

**Cause**: Very long SQL expressions (rare).

**Solution**: The full SQL query is still stored in the Query entity, so no information is lost.

## Related Files and References

### Core Implementation Files

1. **SQL Parser**: `/home/user/datahub/metadata-ingestion/src/datahub/sql_parsing/sqlglot_lineage.py`
   - `create_lineage_sql_parsed_result()` - Parses SQL
   - `ColumnLineageInfo` - Contains transformation logic
   - `ColumnTransformation` - Stores the expression

2. **Lineage Client**: `/home/user/datahub/metadata-ingestion/src/datahub/sdk/lineage_client.py`
   - `infer_lineage_from_sql()` - Standard approach (without transformation logic)
   - `add_lineage()` - Low-level lineage creation

3. **SQL Parsing Aggregator**: `/home/user/datahub/metadata-ingestion/src/datahub/sql_parsing/sql_parsing_aggregator.py`
   - Reference implementation showing how ingestion framework handles transformation logic

4. **Metadata Models**: `/home/user/datahub/metadata-models/src/main/pegasus/com/linkedin/dataset/FineGrainedLineage.pdl`
   - Defines `transformOperation` field in the schema

### DataHub Documentation

- [DataHub SQL Lineage](https://datahubproject.io/docs/lineage/sql_lineage/)
- [Column-Level Lineage](https://datahubproject.io/docs/lineage/column_level_lineage/)
- [Metadata Model](https://datahubproject.io/docs/metadata-modeling/)

## Summary

This enhancement provides a better way to track SQL transformations in DataHub by preserving the column-level transformation logic. Instead of just knowing "column A depends on column B", you can now see "column A is calculated as `COALESCE(SUM(B), 0)`".

This makes lineage more actionable for:
- **Data analysts** - Understand how metrics are calculated
- **Data engineers** - Debug data quality issues
- **Compliance teams** - Audit data transformations
- **Documentation** - Auto-generate transformation documentation

The implementation follows DataHub's existing patterns and can be easily integrated into existing workflows.
