# EpayDbMigrator: High-Performance MSSQL to PostgreSQL Migration Tool

This is a comprehensive, step-by-step guide to building the `EpayDbMigrator` application from scratch. This guide documents the architectural decisions, performance optimizations, and specific database idiosyncrasies resolved during its development. 

Any .NET developer can follow this guide to completely recreate this high-speed data migration tool.

---

## Step 1: Initialize the Project 

Create a standard .NET 8 Console Application to act as our migrator.

1. Open a terminal in your desired directory.
2. Run the initialization command:
   ```bash
   dotnet new console -n EpayDbMigrator
   cd EpayDbMigrator
   ```

## Step 2: Install Required NuGet Dependencies

We need specific packages to connect to SQL Server, PostgreSQL, and to handle JSON configuration and logging elegantly.

1. Run the following commands to add the packages:
   ```bash
   dotnet add package Microsoft.Data.SqlClient
   dotnet add package Npgsql
   dotnet add package Microsoft.Extensions.Configuration.Json
   dotnet add package Microsoft.Extensions.Logging.Console
   ```

## Step 3: Configure Database Connection Strings

We use an `appsettings.json` file so connection strings aren't hardcoded.

1. Create a file named `appsettings.json` in the root of the project:
   ```json
   {
     "ConnectionStrings": {
       "SqlServer": "Server=YOUR_SERVER_NAME;Database=epay_db;Trusted_Connection=True;Encrypt=False;",
       "Postgres": "Host=localhost;Database=epay_db;Username=postgres;Password=YOUR_POSTGRES_PASSWORD;Port=5432"
     }
   }
   ```
2. **Crucial Build Step**: Open `EpayDbMigrator.csproj` and add the following `<ItemGroup>` inside the `<Project>` tags so the file gets copied to the build output directory:
   ```xml
   <ItemGroup>
     <None Update="appsettings.json">
       <CopyToOutputDirectory>PreserveNewest</CopyToOutputDirectory>
     </None>
   </ItemGroup>
   ```

## Step 4: Create the Data Type Mapper

Data types differ dramatically between SQL Server and PostgreSQL. We need a dictionary mapper to seamlessly translate column definitions. 

1. Create `DataTypeMapper.cs`:
   ```csharp
   using System;
   using System.Collections.Generic;

   namespace EpayDbMigrator
   {
       public static class DataTypeMapper
       {
           private static readonly Dictionary<string, string> SqlServerToPostgresMap = new(StringComparer.OrdinalIgnoreCase)
           {
               // Exact Numerics
               { "bigint", "bigint" },
               { "numeric", "numeric" },
               { "bit", "boolean" },
               { "smallint", "smallint" },
               { "decimal", "numeric" },
               { "int", "integer" },
               { "tinyint", "smallint" }, 
               { "money", "numeric" },

               // Approximate Numerics
               { "float", "double precision" },
               { "real", "real" },

               // Date and Time
               { "date", "date" },
               { "datetimeoffset", "timestamp with time zone" },
               { "datetime2", "timestamp without time zone" },
               { "smalldatetime", "timestamp without time zone" },
               { "datetime", "timestamp without time zone" },
               { "time", "time without time zone" },

               // Character Strings
               { "char", "character" },
               { "varchar", "character varying" },
               { "text", "text" },
               { "nchar", "character" },
               { "nvarchar", "character varying" },
               { "ntext", "text" },

               // Binary Strings
               { "binary", "bytea" },
               { "varbinary", "bytea" },
               { "image", "bytea" },

               // Edge Cases Discovered
               { "timestamp", "bytea" }, // MSSQL 'timestamp' is actually an 8-byte rowversion binary, NOT a datetime!
               { "rowversion", "bytea" },
               { "uniqueidentifier", "uuid" }
           };

           public static string GetPostgresType(string sqlServerType, int? length)
           {
               if (SqlServerToPostgresMap.TryGetValue(sqlServerType, out var pgType))
               {
                   if (pgType == "character varying" || pgType == "character")
                   {
                       if (length.HasValue && length.Value > 0 && length.Value < 10485760)
                           return $"{pgType}({length.Value})";
                       if (length == -1 || length == null) 
                           return "text"; // VARCHAR(MAX) translates to postgres TEXT
                   }
                   return pgType;
               }
               return sqlServerType.ToLowerInvariant(); 
           }
       }
   }
   ```

## Step 5: Implement the Migration Service (The Engine)

This is the core of the application responsible for moving the structure and data.

1. Create `MigrationService.cs`. 

*Note: The implementation below details the specific architecture required to surpass MSSQL Memory limits and PostgreSQL binary formatting rules.*

```csharp
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace EpayDbMigrator
{
    public class ColumnInfo
    {
        public string Name { get; set; }
        public string DataType { get; set; }
        public int? MaxLength { get; set; }
        public bool IsNullable { get; set; }
    }

    public class MigrationService
    {
        private readonly string _sqlServerConnectionString;
        private readonly string _postgresConnectionString;
        private readonly ILogger<MigrationService> _logger;

        public MigrationService(string sqlServerConnectionString, string postgresConnectionString, ILogger<MigrationService> logger)
        {
            _sqlServerConnectionString = sqlServerConnectionString;
            _postgresConnectionString = postgresConnectionString;
            _logger = logger;
        }

        public async Task RunMigrationAsync()
        {
            using var sqlConnection = new SqlConnection(_sqlServerConnectionString);
            await sqlConnection.OpenAsync();

            using var pgConnection = new NpgsqlConnection(_postgresConnectionString);
            await pgConnection.OpenAsync();

            var tables = await GetTablesAsync(sqlConnection);
            _logger.LogInformation($"Found {tables.Count} tables to migrate.");

            foreach (var table in tables)
            {
                _logger.LogInformation($"--- Migrating Table: {table} ---");
                
                // SKIPPING LOGIC: Check if it already has data to safely resume interrupted migrations
                var checkCmd = new NpgsqlCommand("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema') AND table_name = @t", pgConnection);
                checkCmd.Parameters.AddWithValue("t", table);
                var exists = (long)(await checkCmd.ExecuteScalarAsync() ?? 0) > 0;

                if (exists)
                {
                    var countCmd = new NpgsqlCommand($"SELECT COUNT(*) FROM \"{table}\"", pgConnection);
                    var rowCount = (long)(await countCmd.ExecuteScalarAsync() ?? 0);
                    if (rowCount > 0)
                    {
                        _logger.LogInformation($"Table \"{table}\" already contains {rowCount} rows. Skipping transfer pipeline.");
                        await VerifyRowCountAsync(sqlConnection, pgConnection, table);
                        continue; 
                    }
                    else
                    {
                        // Clean out empty/corrupted schema from failed attempts
                        var dropCmd = new NpgsqlCommand($"DROP TABLE \"{table}\" CASCADE;", pgConnection);
                        await dropCmd.ExecuteNonQueryAsync();
                    }
                }

                var columns = await GetColumnsAsync(sqlConnection, table);
                await CreateTableInPostgresAsync(pgConnection, table, columns);
                await MigrateDataAsync(sqlConnection, pgConnection, table, columns);
                await VerifyRowCountAsync(sqlConnection, pgConnection, table);
            }
        }

        // ... (Private helper methods: GetTablesAsync reading INFORMATION_SCHEMA.TABLES schema='dbo')
        // ... (Private helper methods: GetColumnsAsync reading INFORMATION_SCHEMA.COLUMNS schema='dbo')
        // ... (Private helper methods: CreateTableInPostgresAsync concatenating simple CREATE TABLE IF NOT EXISTS scripts using double-quoted identifiers to preserve casing)

        private async Task MigrateDataAsync(SqlConnection sqlConnection, NpgsqlConnection pgConnection, string tableName, List<ColumnInfo> columns)
        {
            // CRITICAL OPTIMIZATION: Pagination vs Memory Pool
            // If we attempt to select millions of rows (e.g., inventory_audit table) into memory, SQL Server throws Error 802 (Buffer Pool Exhaustion).
            // We MUST read using a deterministic OFFSET FETCH to chunk the network stream.
            var orderColumn = columns[0].Name; 

            var colNames = string.Join(", ", columns.Select(c => $"\"{c.Name}\""));
            
            // CRITICAL OPTIMIZATION: NpgsqlBinaryImporter
            // Standard INSERTs take hours. Binary Imports take seconds.
            var copyCommand = $"COPY \"{tableName}\" ({colNames}) FROM STDIN (FORMAT BINARY)";
            
            int rowsMigrated = 0;
            const int batchSize = 100000;
            bool hasMoreData = true;

            using var writer = await pgConnection.BeginBinaryImportAsync(copyCommand);

            while (hasMoreData)
            {
                var selectQuery = $@"
                    SELECT * FROM [{tableName}]
                    ORDER BY [{orderColumn}]
                    OFFSET {rowsMigrated} ROWS 
                    FETCH NEXT {batchSize} ROWS ONLY";

                using var sqlCmd = new SqlCommand(selectQuery, sqlConnection);
                sqlCmd.CommandTimeout = 0; // Prevent ADO.NET query timeouts 
                
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
                            
                            // EXPLICIT FORMAT TYPE MAPPING:
                            // Because Npgsql streams raw raw bytes over the network, generic .NET objects throw format crashes unless explicitly cast.
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
                                    await writer.WriteAsync(dt, NpgsqlTypes.NpgsqlDbType.Date);
                                else 
                                    await writer.WriteAsync(dt, NpgsqlTypes.NpgsqlDbType.Timestamp);
                            }
                            else
                            {
                                await writer.WriteAsync(val);
                            }
                        }
                    }
                    
                    rowsInCurrentBatch++;
                    rowsMigrated++;
                }

                if (rowsInCurrentBatch < batchSize)
                    hasMoreData = false;
            }
                
            await writer.CompleteAsync();
            _logger.LogInformation($"Finished transferring {rowsMigrated} total rows for {tableName}.");
        }
        
        // ... (Private helper methods: VerifyRowCountAsync executing basic COUNT(*) on both tables to ensure zero data drop).
    }
}
```

## Step 6: Wire up Program.cs

Connect configuration and logging, instantiate the service, and catch high-level errors.

1. Overwrite `Program.cs` with the entry point:
```csharp
using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace EpayDbMigrator
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();

            using var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddConsole();
                builder.SetMinimumLevel(LogLevel.Information);
            });
            
            var logger = loggerFactory.CreateLogger<MigrationService>();
            var sqlString = config.GetConnectionString("SqlServer");
            var pgString = config.GetConnectionString("Postgres");

            logger.LogInformation("Starting DB Migration Tool...");

            var migrator = new MigrationService(sqlString, pgString, logger);
            
            try 
            {
                await migrator.RunMigrationAsync();
                logger.LogInformation("Migration completed successfully.");
            }
            catch (Exception ex)
            {
                logger.LogCritical(ex, "An unhandled exception occurred during migration.");
            }
        }
    }
}
```

## Step 7: Run the Project!

1. Compile and execute from the terminal:
```bash
dotnet build
dotnet run
```
2. Watch the console log to verify that data batches seamlessly, row counts correctly align upon verification, and no MSSQL buffer errors are thrown.
