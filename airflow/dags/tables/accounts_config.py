tables_config = [
    {
        "table_name": "accounts",
        "postgres_schema": "public",
        "snowflake_schema": "PUBLIC",
        "snowflake_database": "MICROHOUSE_RAW",
        "primary_key": "account_id",
        "load_type": "full",  # 'full' or 'incremental'
        "incremental_column": None,  # For incremental loads (e.g., 'updated_at')
    },
    {
        "table_name": "account_types",
        "postgres_schema": "public",
        "snowflake_schema": "PUBLIC",
        "snowflake_database": "MICROHOUSE_RAW",
        "primary_key": "account_type_id",
        #"load_type": "incremental",
        "load_type": "full",
        "incremental_column": None,
    },
]


# Batch size for data transfer
batch_size = 10000