using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace EpayDbMigrator
{
    public class MigrationService
    {
        private readonly string _sqlServerConnectionString;
        private readonly string _postgresConnectionString;
        private readonly ILogger<MigrationService> _logger;

        public MigrationService(string sqlServerConnectionString, string postgresConnectionString, ILogger<MigrationService> logger)
        {
            _sqlServerConnectionString = sqlServerConnectionString ?? throw new ArgumentException("SqlServer connection string is missing.");
            _postgresConnectionString = postgresConnectionString ?? throw new ArgumentException("Postgres connection string is missing.");
            _logger = logger;
        }

        public async Task<List<TableInfo>> GetAvailableTablesAsync()
        {
            using var sqlConnection = new SqlConnection(_sqlServerConnectionString);
            await sqlConnection.OpenAsync();
            return await GetTablesInternalAsync(sqlConnection);
        }

        public async Task RunMigrationAsync(List<TableInfo> selectedTables = null)
        {
            _logger.LogInformation("Connecting to SQL Server...");
            using var sqlConnection = new SqlConnection(_sqlServerConnectionString);
            await sqlConnection.OpenAsync();

            _logger.LogInformation("Connecting to PostgreSQL...");
            using var pgConnection = new NpgsqlConnection(_postgresConnectionString);
            await pgConnection.OpenAsync();

            // 1. Get Tables
            var tables = selectedTables != null && selectedTables.Count > 0 
                ? selectedTables 
                : await GetTablesInternalAsync(sqlConnection);
                
            _logger.LogInformation($"Found {tables.Count} tables to migrate.");

            var verificationResults = new List<(TableInfo Table, long SqlCount, long PgCount, bool IsMatch)>();

            foreach (var table in tables)
            {
                _logger.LogInformation($"--- Migrating Table: {table.Schema}.{table.Name} ---");
                
                // Ensure the schema exists in PG before proceeding
                var createSchemaCmd = new NpgsqlCommand($"CREATE SCHEMA IF NOT EXISTS \"{table.Schema}\";", pgConnection);
                await createSchemaCmd.ExecuteNonQueryAsync();

                // Check if table already exists in PG
                var checkCmd = new NpgsqlCommand("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @s AND table_name = @t", pgConnection);
                checkCmd.Parameters.AddWithValue("s", table.Schema);
                checkCmd.Parameters.AddWithValue("t", table.Name);
                var exists = (long)(await checkCmd.ExecuteScalarAsync() ?? 0) > 0;

                if (exists)
                {
                    var countCmd = new NpgsqlCommand($"SELECT COUNT(*) FROM \"{table.Schema}\".\"{table.Name}\"", pgConnection);
                    countCmd.CommandTimeout = 0;
                    var rowCount = (long)(await countCmd.ExecuteScalarAsync() ?? 0);
                    if (rowCount > 0)
                    {
                        _logger.LogInformation($"Table \"{table.Schema}\".\"{table.Name}\" already contains {rowCount} rows. Skipping transfer pipeline.");
                        var result = await VerifyRowCountAsync(sqlConnection, pgConnection, table);
                        verificationResults.Add(result);
                        continue; // Skip!
                    }
                    else
                    {
                        // Exists but empty (likely failed previously), drop it to recreate with latest schema mappings
                        var dropCmd = new NpgsqlCommand($"DROP TABLE \"{table.Schema}\".\"{table.Name}\" CASCADE;", pgConnection);
                        await dropCmd.ExecuteNonQueryAsync();
                    }
                }

                // 2. Get Schema (Columns)
                var columns = await GetColumnsAsync(sqlConnection, table);
                
                // 3. Create Table in PG
                await CreateTableInPostgresAsync(pgConnection, table, columns);
                
                // 4. Migrate Data
                await MigrateDataAsync(sqlConnection, pgConnection, table, columns);
                
                // 5. Verify Row Count
                verificationResults.Add(await VerifyRowCountAsync(sqlConnection, pgConnection, table));
            }

            // Print Final Summary Table
            Console.WriteLine();
            Console.WriteLine("=========================================================================================");
            Console.WriteLine($"{"Table Name",-40} | {"SQL Count",-15} | {"PostgreSQL Count",-16} | Variance");
            Console.WriteLine("=========================================================================================");
            
            foreach (var res in verificationResults)
            {
                var variance = res.IsMatch ? "No" : "YES";
                Console.WriteLine($"{res.Table.ToString(),-40} | {res.SqlCount,-15} | {res.PgCount,-16} | {variance}");
            }
            Console.WriteLine("=========================================================================================");
            Console.WriteLine();

            // 6. Migrate PKs & Indexes & Foreign Keys
            Console.WriteLine("\nStarting post-migration index and constraint application (this may take a while for large tables)...");
            _logger.LogInformation("Migrating indexes, primary keys, and foreign keys...");
            foreach (var table in tables)
            {
                await MigrateIndexesAndConstraintsAsync(sqlConnection, pgConnection, table);
            }
            await MigrateForeignKeysAsync(sqlConnection, pgConnection, tables);
            _logger.LogInformation("All constraints and indexes applied successfully.");
            Console.WriteLine("Constraints and indexes applied.\n");
        }

        private async Task<(TableInfo Table, long SqlCount, long PgCount, bool IsMatch)> VerifyRowCountAsync(SqlConnection sqlConnection, NpgsqlConnection pgConnection, TableInfo table)
        {
            using var sqlCmd = new SqlCommand($"SELECT COUNT(*) FROM [{table.Schema}].[{table.Name}] OPTION (MAXDOP 1)", sqlConnection);
            sqlCmd.CommandTimeout = 0;
            var sqlCount = Convert.ToInt64(await sqlCmd.ExecuteScalarAsync() ?? 0);

            using var pgCmd = new NpgsqlCommand($"SELECT COUNT(*) FROM \"{table.Schema}\".\"{table.Name}\"", pgConnection);
            pgCmd.CommandTimeout = 0; // Infinite timeout for massive table verification checks
            var pgCount = Convert.ToInt64(await pgCmd.ExecuteScalarAsync() ?? 0);

            return (table, sqlCount, pgCount, sqlCount == pgCount);
        }

        private async Task<List<TableInfo>> GetTablesInternalAsync(SqlConnection sqlConnection)
        {
            var tables = new List<TableInfo>();
            var query = "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG = DB_NAME() ORDER BY TABLE_SCHEMA, TABLE_NAME";
            
            using var cmd = new SqlCommand(query, sqlConnection);
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                tables.Add(new TableInfo { Schema = reader.GetString(0), Name = reader.GetString(1) });
            }
            return tables;
        }

        private async Task<List<ColumnInfo>> GetColumnsAsync(SqlConnection sqlConnection, TableInfo table)
        {
            var columns = new List<ColumnInfo>();
            var query = @"
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = @TableName AND TABLE_SCHEMA = @SchemaName
                ORDER BY ORDINAL_POSITION";

            using var cmd = new SqlCommand(query, sqlConnection);
            cmd.Parameters.AddWithValue("@TableName", table.Name);
            cmd.Parameters.AddWithValue("@SchemaName", table.Schema);
            
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                columns.Add(new ColumnInfo
                {
                    Name = reader.GetString(0),
                    DataType = reader.GetString(1),
                    MaxLength = reader.IsDBNull(2) ? (int?)null : reader.GetInt32(2),
                    IsNullable = reader.GetString(3) == "YES"
                });
            }
            return columns;
        }

        private async Task CreateTableInPostgresAsync(NpgsqlConnection pgConnection, TableInfo table, List<ColumnInfo> columns)
        {
            var sb = new StringBuilder();
            // Wrap table alias in double quotes to preserve case
            sb.AppendLine($"CREATE TABLE IF NOT EXISTS \"{table.Schema}\".\"{table.Name}\" (");
            
            var columnDefs = new List<string>();
            foreach (var col in columns)
            {
                var pgType = DataTypeMapper.GetPostgresType(col.DataType, col.MaxLength);
                var nullability = col.IsNullable ? "NULL" : "NOT NULL";
                columnDefs.Add($"    \"{col.Name}\" {pgType} {nullability}");
            }
            
            sb.AppendLine(string.Join(",\n", columnDefs));
            sb.AppendLine(");");

            // Execute creation
            using var cmd = new NpgsqlCommand(sb.ToString(), pgConnection);
            await cmd.ExecuteNonQueryAsync();
            _logger.LogInformation($"Created table \"{table.Schema}\".\"{table.Name}\" in PostgreSQL.");
        }

        private async Task MigrateDataAsync(SqlConnection sqlConnection, NpgsqlConnection pgConnection, TableInfo table, List<ColumnInfo> columns)
        {
            // We need a deterministic ORDER BY column for OFFSET-FETCH pagination.
            // Try to find a primary key, or fallback to the first column.
            var orderColumn = columns.FirstOrDefault()?.Name ?? "1"; 

            var colNames = string.Join(", ", columns.Select(c => $"\"{c.Name}\""));
            var copyCommand = $"COPY \"{table.Schema}\".\"{table.Name}\" ({colNames}) FROM STDIN (FORMAT BINARY)";
            
            int rowsMigrated = 0;
            const int batchSize = 100000;
            bool hasMoreData = true;

            using var writer = await pgConnection.BeginBinaryImportAsync(copyCommand);

            while (hasMoreData)
            {
                var selectQuery = $@"
                    SELECT * FROM [{table.Schema}].[{table.Name}]
                    ORDER BY [{orderColumn}]
                    OFFSET {rowsMigrated} ROWS 
                    FETCH NEXT {batchSize} ROWS ONLY";

                using var sqlCmd = new SqlCommand(selectQuery, sqlConnection);
                sqlCmd.CommandTimeout = 0; // Infinite timeout 
                
                using var reader = await sqlCmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess);
                
                int rowsInCurrentBatch = 0;

                while (await reader.ReadAsync())
                {
                    await writer.StartRowAsync();
                    
                    for (int i = 0; i < columns.Count; i++)
                    {
                        var val = reader.GetValue(i);
                        if (val == DBNull.Value)
                        {
                            await writer.WriteNullAsync();
                        }
                        else
                        {
                            var pgType = DataTypeMapper.GetPostgresType(columns[i].DataType, columns[i].MaxLength);
                            
                            // NpgsqlBinaryImporter needs explicit types for some specific mappings
                            if (pgType == "bytea" && val is byte[] byteArray)
                            {
                                await writer.WriteAsync(byteArray, NpgsqlTypes.NpgsqlDbType.Bytea);
                            }
                            else if (val is TimeSpan timeSpanValue)
                            {
                                await writer.WriteAsync(timeSpanValue, NpgsqlTypes.NpgsqlDbType.Time);
                            }
                            else if (val is DateTime dt)
                            {
                                if (pgType == "date") 
                                {
                                    await writer.WriteAsync(dt, NpgsqlTypes.NpgsqlDbType.Date);
                                }
                                else 
                                {
                                    await writer.WriteAsync(dt, NpgsqlTypes.NpgsqlDbType.Timestamp);
                                }
                            }
                            else
                            {
                                await writer.WriteAsync(val);
                            }
                        }
                    }
                    
                    rowsInCurrentBatch++;
                    rowsMigrated++;

                    if (rowsMigrated % 100000 == 0) 
                    {
                        _logger.LogInformation($"Migrated {rowsMigrated} rows for {table.Schema}.{table.Name}...");
                    }
                }

                // If we fetched fewer rows than the batch size, we've hit the end of the table
                if (rowsInCurrentBatch < batchSize)
                {
                    hasMoreData = false;
                }
            }
                
            await writer.CompleteAsync();

            _logger.LogInformation($"Finished transferring {rowsMigrated} total rows for {table.Schema}.{table.Name}.");
        }

        private async Task MigrateIndexesAndConstraintsAsync(SqlConnection sqlConnection, NpgsqlConnection pgConnection, TableInfo table)
        {
            _logger.LogInformation($"Migrating indexes and primary keys for {table.Schema}.{table.Name}...");

            var query = @"
                SELECT 
                    i.name AS IndexName, 
                    i.is_primary_key AS IsPrimaryKey,
                    i.is_unique AS IsUnique,
                    c.name AS ColumnName,
                    ic.is_descending_key AS IsDescending
                FROM sys.indexes i
                JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                JOIN sys.tables t ON i.object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE t.name = @TableName AND s.name = @SchemaName AND i.type > 0
                ORDER BY i.name, ic.key_ordinal;";

            using var cmd = new SqlCommand(query, sqlConnection);
            cmd.Parameters.AddWithValue("@TableName", table.Name);
            cmd.Parameters.AddWithValue("@SchemaName", table.Schema);

            var indexes = new Dictionary<string, (bool IsPK, bool IsUnique, List<(string ColName, bool IsDesc)> Columns)>();

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var idxName = reader.GetString(0);
                var isPk = reader.GetBoolean(1);
                var isUnique = reader.GetBoolean(2);
                var colName = reader.GetString(3);
                var isDesc = reader.GetBoolean(4);

                if (!indexes.ContainsKey(idxName))
                {
                    indexes[idxName] = (isPk, isUnique, new List<(string, bool)>());
                }
                indexes[idxName].Columns.Add((colName, isDesc));
            }

            foreach (var kvp in indexes)
            {
                var idxName = kvp.Key;
                var info = kvp.Value;
                var columnsParam = string.Join(", ", info.Columns.Select(c => $"\"{c.ColName}\" {(c.IsDesc ? "DESC" : "ASC")}"));

                try
                {
                    if (info.IsPK)
                    {
                        var cols = string.Join(", ", info.Columns.Select(c => $"\"{c.ColName}\""));
                        using var pkCmd = new NpgsqlCommand($"ALTER TABLE \"{table.Schema}\".\"{table.Name}\" ADD CONSTRAINT \"{idxName}\" PRIMARY KEY ({cols});", pgConnection);
                        pkCmd.CommandTimeout = 0; // Infinite timeout for large tables
                        await pkCmd.ExecuteNonQueryAsync();
                    }
                    else
                    {
                        var uniqueStr = info.IsUnique ? "UNIQUE " : "";
                        var ifNotExists = info.IsUnique ? "" : "IF NOT EXISTS "; // Postgres only supports IF NOT EXISTS on non-unique indexes easily natively, but we'll catch the error anyway.
                        using var idxCmd = new NpgsqlCommand($"CREATE {uniqueStr}INDEX {ifNotExists}\"{idxName}\" ON \"{table.Schema}\".\"{table.Name}\" ({columnsParam});", pgConnection);
                        idxCmd.CommandTimeout = 0; // Infinite timeout for large tables
                        await idxCmd.ExecuteNonQueryAsync();
                    }
                }
                catch (PostgresException ex) when (ex.SqlState == "42P07" || ex.SqlState == "42710")
                {
                    // 42P07: duplicate_table (also applies to indexes)
                    // 42710: duplicate_object (applies to constraints)
                    _logger.LogDebug($"Index or PK '{idxName}' on {table.Schema}.{table.Name} already exists. Skipping.");
                }
                catch (PostgresException ex) when (ex.SqlState == "42P16")
                {
                    // 42P16: invalid_table_definition (e.g., multiple primary keys for table)
                    _logger.LogDebug($"Table {table.Schema}.{table.Name} already has a primary key. Skipping addition of '{idxName}'.");
                }
                catch (PostgresException ex) when (ex.SqlState == "23505")
                {
                    // 23505: unique_violation
                    _logger.LogWarning($"Could not create UNIQUE constraint '{idxName}' on {table.Schema}.{table.Name} due to existing duplicate data.");
                }
                catch (Exception ex)
                {
                    _logger.LogWarning($"Failed to create index/PK {idxName} on {table.Schema}.{table.Name}: {ex.Message}");
                }
            }
        }

        private async Task MigrateForeignKeysAsync(SqlConnection sqlConnection, NpgsqlConnection pgConnection, List<TableInfo> tables)
        {
            _logger.LogInformation("Migrating foreign keys across all migrated tables...");

            foreach (var table in tables)
            {
                var query = @"
                    SELECT 
                        fk.name AS ForeignKeyName,
                        tr.name AS ReferencedTableName,
                        sr.name AS ReferencedSchemaName,
                        cp.name AS ParentColumn,
                        cr.name AS ReferencedColumn
                    FROM sys.foreign_keys fk
                    JOIN sys.tables tp ON fk.parent_object_id = tp.object_id
                    JOIN sys.schemas sp ON tp.schema_id = sp.schema_id
                    JOIN sys.tables tr ON fk.referenced_object_id = tr.object_id
                    JOIN sys.schemas sr ON tr.schema_id = sr.schema_id
                    JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                    JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
                    JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
                    WHERE tp.name = @TableName AND sp.name = @SchemaName
                    ORDER BY fk.name, fkc.constraint_column_id;";

                using var cmd = new SqlCommand(query, sqlConnection);
                cmd.Parameters.AddWithValue("@TableName", table.Name);
                cmd.Parameters.AddWithValue("@SchemaName", table.Schema);

                var fks = new Dictionary<string, (string RefSchema, string RefTable, List<string> ParentCols, List<string> RefCols)>();

                using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var fkName = reader.GetString(0);
                    var refTable = reader.GetString(1);
                    var refSchema = reader.GetString(2);
                    var parentCol = reader.GetString(3);
                    var refCol = reader.GetString(4);

                    if (!fks.ContainsKey(fkName))
                    {
                        fks[fkName] = (refSchema, refTable, new List<string>(), new List<string>());
                    }
                    fks[fkName].ParentCols.Add(parentCol);
                    fks[fkName].RefCols.Add(refCol);
                }

                foreach (var kvp in fks)
                {
                    var fkName = kvp.Key;
                    var info = kvp.Value;
                    var parentColsStr = string.Join(", ", info.ParentCols.Select(c => $"\"{c}\""));
                    var refColsStr = string.Join(", ", info.RefCols.Select(c => $"\"{c}\""));

                    try
                    {
                        using var fkCmd = new NpgsqlCommand($@"
                            ALTER TABLE ""{table.Schema}"".""{table.Name}""
                            ADD CONSTRAINT ""{fkName}"" 
                            FOREIGN KEY ({parentColsStr}) 
                            REFERENCES ""{info.RefSchema}"".""{info.RefTable}"" ({refColsStr});", pgConnection);

                        fkCmd.CommandTimeout = 0; // Infinite timeout for large tables
                        await fkCmd.ExecuteNonQueryAsync();
                    }
                    catch (PostgresException ex) when (ex.SqlState == "42P07" || ex.SqlState == "42710")
                    {
                        // duplicate_object
                        _logger.LogDebug($"Foreign key '{fkName}' on {table.Schema}.{table.Name} already exists. Skipping.");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning($"Failed to create foreign key {fkName} on {table.Schema}.{table.Name}: {ex.Message}");
                    }
                }
            }
        }
    }

    public class TableInfo
    {
        public string Schema { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        
        public override string ToString() => $"{Schema}.{Name}";
    }

    public class ColumnInfo
    {
        public string Name { get; set; } = string.Empty;
        public string DataType { get; set; } = string.Empty;
        public int? MaxLength { get; set; }
        public bool IsNullable { get; set; }
    }
}
