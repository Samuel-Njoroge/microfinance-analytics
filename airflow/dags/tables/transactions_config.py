tables_config = [
    {
        "table_name": "transactions",
        "postgres_schema": "public",
        "snowflake_schema": "PUBLIC",
        "snowflake_database": "MICROHOUSE_RAW",
        "primary_key": "transaction_id",
        "load_type": "full",  # 'full' or 'incremental'
        "incremental_column": None,  # For incremental loads (e.g., 'updated_at')
    },
    {
        "table_name": "transaction_types",
        "postgres_schema": "public",
        "snowflake_schema": "PUBLIC",
        "snowflake_database": "MICROHOUSE_RAW",
        "primary_key": "transaction_type_id",
        #"load_type": "incremental",
        "load_type": "full",
        "incremental_column": None,
    },
    {
        "table_name": "transfers",
        "postgres_schema": "public",
        "snowflake_schema": "PUBLIC",
        "snowflake_database": "MICROHOUSE_RAW",
        "primary_key": "transfer_id",
        "load_type": "full",  # 'full' or 'incremental'
        "incremental_column": None,  # For incremental loads (e.g., 'updated_at')
    },
]

# Batch size for data transfer
batch_size = 10000