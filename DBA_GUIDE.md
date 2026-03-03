# DBA Guide: EpayDbMigrator Database Logic

This guide provides Database Administrators (DBAs) with a detailed breakdown of the exact SQL queries and commands executed by the `EpayDbMigrator` application against both the source Microsoft SQL Server and the destination PostgreSQL database.

Understanding this logic is critical for DBAs to evaluate the security, performance impact, and behavior of the migration tool.

---

## 1. Schema Discovery (MSSQL)

To understand what needs to be migrated, the tool dynamically queries SQL Server's internal schema views.

### Discovering Tables
The migrator first queries `INFORMATION_SCHEMA.TABLES` to find all base tables in the `dbo` schema.
**Executed Query:**
```sql
SELECT TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE = 'BASE TABLE' 
  AND TABLE_CATALOG = DB_NAME() 
  AND TABLE_SCHEMA = 'dbo';
```

### Discovering Columns
For every table found, the migrator queries `INFORMATION_SCHEMA.COLUMNS` to get the column names, data types, lengths, and nullability. It orders by `ORDINAL_POSITION` to ensure the structure matches exactly during creation.
**Executed Query:**
```sql
SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = @TableName 
  AND TABLE_SCHEMA = 'dbo'
ORDER BY ORDINAL_POSITION;
```

---

## 2. Pre-Flight Checks & Table Creation (PostgreSQL)

Before moving any data, the tool prepares the destination database.

### Checking for Existing Tables
The migrator checks if a table has already been migrated by querying Postgres' `information_schema`. It specifically excludes system databases (`pg_catalog`, `information_schema`) because the target user may default to a schema like `dbo` or `public`.
**Executed Query:**
```sql
SELECT COUNT(*) 
FROM information_schema.tables 
WHERE table_schema NOT IN ('pg_catalog', 'information_schema') 
  AND table_name = @TableName;
```

If the table exists, it checks if it has rows:
```sql
SELECT COUNT(*) FROM "table_name";
```
- **If rows > 0**: The transfer is completely skipped to avoid data duplication.
- **If rows = 0**: The table is aggressively dropped (`DROP TABLE "table_name" CASCADE;`) to clear out any corrupted or incomplete schema from a previously failed run.

### Creating the Table
If the table needs to be created, the tool uses the column metadata harvested from MSSQL, runs it through an internal C# Type Mapper (e.g., converting `datetime` to `timestamp without time zone`), and executes a standard `CREATE TABLE` command using double-quotes to strictly preserve casing.
**Executed Query (Example):**
```sql
CREATE TABLE IF NOT EXISTS "devices" (
    "id" integer NOT NULL,
    "device_name" character varying(255) NOT NULL,
    "created_on" timestamp without time zone NOT NULL,
    "reconcile_time" bytea NULL
);
```

---

## 3. High-Performance Data Extraction (MSSQL)

The migrator is specifically optimized so it never exhausts the SQL Server Buffer Pool Memory (Error 802) when dealing with multi-million row tables.

### Paginated Extraction
Instead of issuing a `SELECT * FROM table`, the tool forces a deterministic sort using the first column (usually the Primary Key) and chunks the data precisely into 100,000-row batches using `OFFSET FETCH`.
**Executed Query (Loops until EOF):**
```sql
SELECT * FROM [table_name]
ORDER BY [first_column_name]
OFFSET {rows_already_migrated} ROWS 
FETCH NEXT 100000 ROWS ONLY;
```
*Note: This query is executed with `CommandTimeout = 0` (Infinite) and `CommandBehavior.SequentialAccess` to stream data efficiently over the network rather than caching the full payload in memory.*

---

## 4. High-Speed Bulk Import (PostgreSQL)

Standard `INSERT` statements are far too slow for database migrations. Instead, the tool uses the `COPY` command to bypass the Postgres query planner entirely and stream data via the binary protocol (`NpgsqlBinaryImporter`).

**Executed Command:**
```sql
COPY "table_name" ("col1", "col2", "col3") FROM STDIN (FORMAT BINARY);
```
The application then pipes the exact byte-arrays from the MSSQL `SqlDataReader` directly into this active `COPY` cursor over the network.

---

## 5. Post-Migration Integrity Checking

Once a table completes its data transfer, the tool immediately fires diagnostic queries against both servers simultaneously to guarantee data integrity.

**MSSQL Executed Query:**
```sql
SELECT COUNT(*) FROM [table_name];
```

**PostgreSQL Executed Query:**
```sql
SELECT COUNT(*) FROM "table_name";
```

The tool compares the resulting values in memory. If they are perfectly matched, the table migration is marked as `[SUCCESS]`. Any discrepancy logs a `[MISMATCH]` warning to the console.
