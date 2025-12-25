tables_config = [
    {
        "table_name": "loans",
        "postgres_schema": "public",
        "snowflake_schema": "PUBLIC",
        "snowflake_database": "MICROHOUSE_RAW",
        "primary_key": "loan_id",
        "load_type": "full",  # 'full' or 'incremental'
        "incremental_column": None,  # For incremental loads (e.g., 'updated_at')
    },
    {
        "table_name": "loan_products",
        "postgres_schema": "public",
        "snowflake_schema": "PUBLIC",
        "snowflake_database": "MICROHOUSE_RAW",
        "primary_key": "loan_product_id",
        #"load_type": "incremental",
        "load_type": "full",
        "incremental_column": None,
    },
    {
        "table_name": "loan_collaterals",
        "postgres_schema": "public",
        "snowflake_schema": "PUBLIC",
        "snowflake_database": "MICROHOUSE_RAW",
        "primary_key": "loan_collateral_id",
        #"load_type": "incremental",
        "load_type": "full",
        "incremental_column": None,
    },
    {
        "table_name": "loan_repayments",
        "postgres_schema": "public",
        "snowflake_schema": "PUBLIC",
        "snowflake_database": "MICROHOUSE_RAW",
        "primary_key": "loan_repayment_id",
        "load_type": "full",  # 'full' or 'incremental'
        "incremental_column": None,  # For incremental loads (e.g., 'updated_at')
    },
]


# Batch size for data transfer
batch_size = 10000