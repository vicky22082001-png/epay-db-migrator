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
            { "smallmoney", "numeric" },
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

            // Unicode Character Strings
            { "nchar", "character" },
            { "nvarchar", "character varying" },
            { "ntext", "text" },

            // Binary Strings
            { "binary", "bytea" },
            { "varbinary", "bytea" },
            { "image", "bytea" },

            // Other Data Types
            { "cursor", "text" }, 
            { "hierarchyid", "bytea" }, 
            { "uniqueidentifier", "uuid" },
            { "sql_variant", "text" },
            { "xml", "xml" },
            { "table", "text" },
            { "timestamp", "bytea" }, // MSSQL timestamp is synonymous with rowversion (8-byte binary), NOT date/time
            { "rowversion", "bytea" },
        };

        public static string GetPostgresType(string sqlServerType, int? length)
        {
            if (SqlServerToPostgresMap.TryGetValue(sqlServerType, out var pgType))
            {
                // Handle length specifications for varchars and chars
                if (pgType == "character varying" || pgType == "character")
                {
                    if (length.HasValue && length.Value > 0 && length.Value < 10485760) // Postgres standard limit is very high, but let's just cap max lengths
                    {
                        return $"{pgType}({length.Value})";
                    }
                    if (length == -1 || length == null) // varchar(max) etc
                    {
                        return "text"; // Convert max to text in Postgres
                    }
                }
                
                return pgType;
            }

            // Fallback strategy: try to use the name directly or throw/warn
            return sqlServerType.ToLowerInvariant(); // Might fail, but gives it a chance
        }
    }
}
