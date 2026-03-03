# How to Run the EpayDbMigrator

This guide explains how to quickly execute the migration tool and what output you should expect to see upon a successful run.

## 1. Prerequisites
- **.NET 8 SDK** installed on the machine running the application.
- Access to the source **Microsoft SQL Server** (e.g., `TEKCSZ-NB115\SQLUAT`).
- Access to the destination **PostgreSQL Server** (e.g., `localhost:5432`).
- The PostgreSQL database (`epay_db`) must already be created (the tool will create the tables inside it).

## 2. Configuration
Before running, ensure your connection strings are correct in the `appsettings.json` file located in the root directory:

```json
{
  "ConnectionStrings": {
    "SqlServer": "Server=TEKCSZ-NB115\\SQLUAT;Database=epay_db;Trusted_Connection=True;Encrypt=False;",
    "Postgres": "Host=localhost;Database=epay_db;Username=postgres;Password=YOUR_PASSWORD;Port=5432"
  }
}
```

## 3. Running the Application
Open a Command Prompt or PowerShell terminal.

1. Navigate to the folder containing the application:
   ```bash
   cd "C:\Users\vignesh.k\mssql to pg\EpayDbMigrator"
   ```

2. Execute the application using the .NET CLI:
   ```bash
   dotnet run
   ```

*(Alternatively, if you have published the application as an executable (`.exe`), you can simply double-click `EpayDbMigrator.exe` or run it from the command line).*

---

## 4. Expected Output
When you run the tool, it will output logs to the console detailing every step of the migration process. 

### Start-Up Phase
First, it connects and discovers the tables:
```text
info: EpayDbMigrator.Program[0]
      Starting DB Migration Tool - MSSQL to PostgreSQL
info: EpayDbMigrator.MigrationService[0]
      Connecting to SQL Server...
info: EpayDbMigrator.MigrationService[0]
      Connecting to PostgreSQL...
info: EpayDbMigrator.MigrationService[0]
      Found 56 tables to migrate.
```

### Table Migration Phase (For each table)
For every table, it checks if it needs to be skipped, creates the schema, migrates the data in memory-safe batches, and verifies the row counts perfectly match.

**Example of a successful large table transfer:**
```text
info: EpayDbMigrator.MigrationService[0]
      --- Migrating Table: inventory_audit ---
info: EpayDbMigrator.MigrationService[0]
      Created table "inventory_audit" in PostgreSQL.
info: EpayDbMigrator.MigrationService[0]
      Migrated 100000 rows for inventory_audit...
info: EpayDbMigrator.MigrationService[0]
      Migrated 200000 rows for inventory_audit...
info: EpayDbMigrator.MigrationService[0]
      ...
info: EpayDbMigrator.MigrationService[0]
      Migrated 3500000 rows for inventory_audit...
info: EpayDbMigrator.MigrationService[0]
      Finished transferring 3543112 total rows for inventory_audit.
info: EpayDbMigrator.MigrationService[0]
      [SUCCESS] Table "inventory_audit" row counts MATCH! (3543112 rows)
```

**Example of a skipped table (if data already exists from a previous run):**
```text
info: EpayDbMigrator.MigrationService[0]
      --- Migrating Table: devices ---
info: EpayDbMigrator.MigrationService[0]
      Table "devices" already contains 240 rows. Skipping transfer pipeline.
info: EpayDbMigrator.MigrationService[0]
      [SUCCESS] Table "devices" row counts MATCH! (240 rows)
```

### Completion Phase
Once all tables are processed, the application exits cleanly:
```text
info: EpayDbMigrator.Program[0]
      Migration completed successfully.
```
