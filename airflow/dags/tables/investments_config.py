tables_config = [
    {
        "table_name": "investments",
        "postgres_schema": "public",
        "snowflake_schema": "PUBLIC",
        "snowflake_database": "MICROHOUSE_RAW",
        "primary_key": "investment_id",
        "load_type": "full",  # 'full' or 'incremental'
        "incremental_column": None,  # For incremental loads (e.g., 'updated_at')
    },
    {
        "table_name": "investment_products",
        "postgres_schema": "public",
        "snowflake_schema": "PUBLIC",
        "snowflake_database": "MICROHOUSE_RAW",
        "primary_key": "investment_product_id",
        #"load_type": "incremental",
        "load_type": "full",
        "incremental_column": None,
    },
    {
        "table_name": "investment_earnings",
        "postgres_schema": "public",
        "snowflake_schema": "PUBLIC",
        "snowflake_database": "MICROHOUSE_RAW",
        "primary_key": "investment_earning_id",
        #"load_type": "incremental",
        "load_type": "full",
        "incremental_column": None,
    },
]

# Batch size for data transfer
batch_size = 10000