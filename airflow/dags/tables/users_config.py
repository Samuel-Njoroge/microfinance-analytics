tables_config = [
    {
        "table_name": "users_user",
        "postgres_schema": "public",
        "snowflake_schema": "PUBLIC",
        "snowflake_database": "MICROHOUSE_RAW",
        "primary_key": "id",
        "load_type": "full",  # 'full' or 'incremental'
        "incremental_column": None,  # For incremental loads (e.g., 'updated_at')
    },

]

# # Global settings
# default_postgres_schema = "public"
# default_snowflake_database = "MY_DATABASE"
# default_snowflake_schema = "PUBLIC"
# default_snowflake_warehouse = "COMPUTE_WH"

# Batch size for data transfer
batch_size = 10000