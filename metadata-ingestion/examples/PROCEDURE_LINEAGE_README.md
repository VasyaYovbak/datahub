# SQL Procedure Lineage Processing

Розширена функціональність для обробки lineage цілих SQL процедур та функцій з підтримкою тимчасових таблиць.

## Огляд

Нова функція `process_procedure_lineage()` дозволяє обробляти складні SQL процедури та функції, розбиваючи їх на окремі операції (ноди) та відслідковуючи lineage між ними, включаючи тимчасові таблиці.

### Основні можливості

✅ **Розбиття процедур на ноди** - автоматична сегментація процедури на окремі операції
✅ **Відслідковування temp tables** - повна підтримка CREATE TEMP TABLE з lineage
✅ **DataFlow/DataJob структура** - представлення процедури як DataFlow з DataJob нодами
✅ **CTE expansion** - розгортання Common Table Expressions в transformation logic
✅ **Backward tracking** - пошук джерел даних через попередні ноди
✅ **Підтримка діалектів** - PostgreSQL та Oracle (через sqlglot)

## Архітектура

```
Procedure (SQL Function/Procedure)
    ↓
DataFlow (представляє всю процедуру)
    ├── DataJob 1: PROCEDURE_START (параметри)
    ├── DataJob 2: TRUNCATE (очистка таблиці)
    ├── DataJob 3: CREATE TEMP TABLE (з lineage)
    ├── DataJob 4: INSERT (використовує temp table)
    └── DataJob 5: UPDATE (фінальна операція)
```

### Типи нод

- `PROCEDURE_START` - початкова нода з параметрами функції
- `CREATE_TEMP_TABLE` - створення тимчасової таблиці
- `INSERT` - операція вставки даних
- `UPDATE` - операція оновлення даних
- `DELETE` - операція видалення даних
- `MERGE` - операція злиття даних
- `TRUNCATE` - операція очистки таблиці
- `UNKNOWN` - невизначений тип операції

## Використання

### Базовий приклад

```python
from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.sql_lineage_enhanced import process_procedure_lineage

# Підключення до DataHub
graph = DataHubGraph(config={"server": "http://localhost:8080"})

# SQL процедура
procedure_sql = """
CREATE OR REPLACE FUNCTION my_procedure()
RETURNS INTEGER AS $$
BEGIN
    CREATE TEMP TABLE temp_data AS
    SELECT id, name FROM source_table;

    INSERT INTO target_table (id, name)
    SELECT id, name FROM temp_data;

    RETURN 0;
END;
$$ LANGUAGE plpgsql;
"""

# Обробка lineage
process_procedure_lineage(
    graph=graph,
    procedure_sql=procedure_sql,
    procedure_name="my_procedure",
    platform="postgres",
    env="PROD",
    default_db="my_database",
    default_schema="public"
)
```

### Розширений приклад з параметрами

```python
process_procedure_lineage(
    graph=graph,
    procedure_sql=complex_procedure,
    procedure_name="calculate_metrics",
    platform="postgres",
    platform_instance="prod-cluster",
    env="PROD",
    default_db="analytics",
    default_schema="public",
    procedure_parameters={
        "start_date": "DATE",
        "end_date": "DATE",
        "user_id": "INTEGER"
    },
    override_dialect="postgres",
    expand_ctes=True,
    replace_aliases=True,
    suppress_warnings=True
)
```

## Параметри функції

| Параметр | Тип | Обов'язковий | Опис |
|----------|-----|--------------|------|
| `graph` | DataHubGraph\|DataHubClient | ✅ | З'єднання з DataHub |
| `procedure_sql` | str | ✅ | Повний SQL код процедури |
| `procedure_name` | str | ✅ | Назва процедури для DataFlow |
| `platform` | str | ✅ | Платформа БД (postgres, oracle) |
| `platform_instance` | str | ❌ | Інстанс платформи |
| `env` | str | ❌ | Середовище (default: "PROD") |
| `default_db` | str | ❌ | База даних за замовчуванням |
| `default_schema` | str | ❌ | Схема за замовчуванням |
| `procedure_parameters` | Dict[str, str] | ❌ | Параметри процедури |
| `override_dialect` | str | ❌ | Перевизначити діалект SQL |
| `expand_ctes` | bool | ❌ | Розгортати CTEs (default: True) |
| `replace_aliases` | bool | ❌ | Замінювати аліаси (default: True) |
| `suppress_warnings` | bool | ❌ | Приховати попередження (default: True) |

## Як працює TempTableTracker

`TempTableTracker` відслідковує тимчасові таблиці протягом виконання процедури:

1. **Реєстрація** - коли зустрічається `CREATE TEMP TABLE`, витягується структура та lineage
2. **Зберігання** - інформація про колонки та їх джерела зберігається
3. **Резолюція** - коли наступна нода використовує temp table, витягується її lineage
4. **Backward tracking** - пошук джерела через ланцюжок нод та temp tables

### Приклад tracking процесу

```
Node 1: CREATE TEMP TABLE temp_metrics AS
        SELECT customer_id, SUM(amount) as total
        FROM orders
        GROUP BY customer_id

    → TempTableTracker зберігає:
      - temp_metrics.customer_id = orders.customer_id
      - temp_metrics.total = SUM(orders.amount)

Node 2: INSERT INTO customer_summary (id, total_spent)
        SELECT customer_id, total FROM temp_metrics

    → Lineage розширюється:
      - customer_summary.id ← temp_metrics.customer_id ← orders.customer_id
      - customer_summary.total_spent ← temp_metrics.total ← SUM(orders.amount)
```

## Обмеження та майбутні покращення

### Поточні обмеження

⚠️ **Вкладені виклики процедур** - поки не підтримуються (запланована підтримка)
⚠️ **Динамічний SQL** - EXECUTE statements потребують окремої обробки
⚠️ **Умовна логіка** - IF/CASE блоки обробляються послідовно

### Заплановані покращення

🔜 Підтримка вкладених викликів процедур
🔜 Відслідковування змінних процедур
🔜 Розширена обробка умовної логіки
🔜 Підтримка додаткових діалектів (MySQL, SQL Server)

## Порівняння з існуючим API

### Стара функція (single statement)

```python
infer_lineage_from_sql_with_enhanced_transformation_logic(
    graph=graph,
    query_text="INSERT INTO t1 SELECT * FROM t2",
    platform="postgres"
)
```

**Обмеження:**
- Тільки одна операція
- Немає підтримки temp tables
- Немає групування операцій

### Нова функція (procedures)

```python
process_procedure_lineage(
    graph=graph,
    procedure_sql=full_procedure_code,
    procedure_name="my_proc",
    platform="postgres"
)
```

**Переваги:**
- Багато операцій в одній процедурі
- Повна підтримка temp tables
- Групування в DataFlow/DataJob
- Backward tracking через ноди

## Troubleshooting

### Проблема: "Failed to parse procedure"

**Рішення:** Переконайтеся що діалект вказаний правильно:

```python
process_procedure_lineage(
    ...,
    platform="postgres",  # або "oracle"
    override_dialect="postgres"
)
```

### Проблема: "Temp table not found"

**Рішення:** Переконайтеся що CREATE TEMP TABLE має SELECT statement:

```sql
-- ✅ Правильно
CREATE TEMP TABLE tmp AS SELECT * FROM source;

-- ❌ Неправильно (поки не підтримується)
CREATE TEMP TABLE tmp (id INT, name TEXT);
INSERT INTO tmp VALUES (1, 'test');
```

### Проблема: "No lineage for temp table columns"

**Перевірте:**
1. Чи має SELECT явні назви колонок
2. Чи доступна схема джерельних таблиць в DataHub
3. Чи правильні default_db та default_schema параметри

## Приклади з реальних use cases

Дивіться повний приклад в файлі `procedure_lineage_example.py`:
- Процедура з множинними temp tables
- CTE expansion
- Складна бізнес-логіка
- Повний lineage tracking

## Додаткові ресурси

- [Оригінальна документація SQL lineage](../src/datahub/sdk/sql_lineage_enhanced.py)
- [DataHub SQL Parsing docs](https://datahubproject.io/docs/lineage/sql-parsing/)
- [sqlglot documentation](https://sqlglot.com/)
