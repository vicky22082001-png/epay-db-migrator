using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
heloo
namespace EpayDbMigrator
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Build configuration
            var configuration = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
                .Build();

            // Set up basic logging
            using var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder
                    .AddConfiguration(configuration.GetSection("Logging"))
                    .AddConsole();
            });

            var logger = loggerFactory.CreateLogger<Program>();

            logger.LogInformation("Starting DB Migration Tool - MSSQL to PostgreSQL");
            
            Console.WriteLine("==================================================");
            Console.WriteLine("    EpayDbMigrator - Interactive Migration");
            Console.WriteLine("==================================================");

            var sqlString = configuration.GetConnectionString("SqlServer");
            var pgString = configuration.GetConnectionString("Postgres");

            if (!string.IsNullOrWhiteSpace(sqlString))
            {
                Console.WriteLine($"\nCurrent MSSQL Connection: {sqlString}");
                Console.Write("Use this MSSQL connection? (Y/N) [Y]: ");
                var mssqlResp = Console.ReadLine();
                if (mssqlResp != null && mssqlResp.Trim().Equals("N", StringComparison.OrdinalIgnoreCase))
                {
                    Console.Write("Enter new MSSQL Connection String: ");
                    sqlString = Console.ReadLine()?.Trim() ?? string.Empty;
                }
            }
            else 
            {
                Console.WriteLine("\n[No default MSSQL connection found in appsettings.json]");
                Console.WriteLine("Example: Server=myServerAddress;Database=myDataBase;User Id=myUsername;Password=myPassword;TrustServerCertificate=True;");
                Console.Write("Enter MSSQL Connection String: ");
                sqlString = Console.ReadLine()?.Trim() ?? string.Empty;
            }

            if (!string.IsNullOrWhiteSpace(pgString))
            {
                Console.WriteLine($"\nCurrent Postgres Connection: {pgString}");
                Console.Write("Use this Postgres connection? (Y/N) [Y]: ");
                var pgResp = Console.ReadLine();
                if (pgResp != null && pgResp.Trim().Equals("N", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine("Example: Host=myServer;Database=myDb;Username=myUser;Password=myPassword");
                    Console.Write("Enter new Postgres Connection String: ");
                    pgString = Console.ReadLine()?.Trim() ?? string.Empty;
                }
            }
            else 
            {
                Console.WriteLine("\n[No default Postgres connection found in appsettings.json]");
                Console.WriteLine("Example: Host=myServer;Database=myDb;Username=myUser;Password=myPassword");
                Console.Write("Enter Postgres Connection String: ");
                pgString = Console.ReadLine()?.Trim() ?? string.Empty;
            }
            
            if (string.IsNullOrWhiteSpace(sqlString) || string.IsNullOrWhiteSpace(pgString))
            {
                logger.LogCritical("Both source and destination connection strings are required. Exiting.");
                Console.WriteLine("Press any key to exit...");
                Console.ReadKey();
                return;
            }

            try
            {
                // Create service with specific connection strings rather than configuration injection
                var migrationService = new MigrationService(sqlString, pgString, loggerFactory.CreateLogger<MigrationService>());
                
                Console.WriteLine("\nConnecting to SQL Server to fetch tables...");
                var tables = await migrationService.GetAvailableTablesAsync();
                
                Console.WriteLine($"\nFound {tables.Count} tables in the source database:");
                for (int i = 0; i < tables.Count; i++)
                {
                    Console.WriteLine($"  {i + 1}. {tables[i]}");
                }

                Console.WriteLine("\nOptions:");
                Console.WriteLine("  A) Migrate ALL tables");
                Console.WriteLine("  S) Select specific tables by comma-separated numbers (e.g., 1, 4, 12)");
                Console.Write("Choose an option: ");
                
                var option = Console.ReadLine()?.Trim().ToUpper();
                System.Collections.Generic.List<TableInfo> selectedTables = null;

                if (option == "S")
                {
                    Console.Write("Enter table numbers: ");
                    var selections = Console.ReadLine()?.Split(',', StringSplitOptions.RemoveEmptyEntries);
                    selectedTables = new System.Collections.Generic.List<TableInfo>();
                    
                    if (selections != null)
                    {
                        foreach (var s in selections)
                        {
                            if (int.TryParse(s.Trim(), out int idx) && idx >= 1 && idx <= tables.Count)
                            {
                                selectedTables.Add(tables[idx - 1]);
                            }
                        }
                    }
                    
                    if (selectedTables.Count == 0)
                    {
                        Console.WriteLine("No valid tables selected. Exiting.");
                        return;
                    }
                    Console.WriteLine($"\nSelected {selectedTables.Count} tables to migrate.");
                }
                else
                {
                    Console.WriteLine("\nProceeding with ALL tables.");
                }

                await migrationService.RunMigrationAsync(selectedTables);
                
                logger.LogInformation("Migration completed successfully.");
            }
            catch (Exception ex)
            {
                logger.LogCritical(ex, "An unhandled exception occurred during migration.");
            }
            
            Console.WriteLine("\nPress any key to exit...");
            Console.ReadKey();
        }
    }
}
