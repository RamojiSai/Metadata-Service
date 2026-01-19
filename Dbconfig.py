"""Database configuration - Table names and constants"""

# Table names
DATASETS_TABLE = "datasets"
COLUMNS_TABLE = "columns"
LINEAGE_TABLE = "lineage"

# Source system types
SOURCE_SYSTEMS = ["MySQL", "MSSQL", "PostgreSQL"]

# Column data types (common ones)
COLUMN_TYPES = [
    "int", "bigint", "smallint", "tinyint",
    "varchar", "char", "text",
    "decimal", "numeric", "float", "double",
    "date", "datetime", "timestamp",
    "boolean", "bool",
    "json", "blob"
]

# Search priority mapping
SEARCH_PRIORITY = {
    "table_name": 1,
    "column_name": 2,
    "schema_name": 3,
    "database_name": 4
}