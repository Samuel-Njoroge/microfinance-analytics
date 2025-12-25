from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.task_group import TaskGroup
import pandas as pd
import logging
from typing import Dict, Optional

import sys
sys.path.append('/opt/airflow/dags/config')
from tables.loans_config import tables_config, batch_size

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def extract_from_postgres(
    table_name: str,
    postgres_schema: str,
    load_type: str,
    incremental_column: Optional[str] = None,
    postgres_conn_id: str = 'postgres_source',
    **context
) -> str:

    logger.info(f"Extracting data from {postgres_schema}.{table_name}")
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    if load_type == 'incremental' and incremental_column:
        ti = context['ti']
        last_load_time = ti.xcom_pull(
            key=f'{table_name}_last_load_time',
            default='1970-01-01 00:00:00'
        )
        query = f"""
            SELECT * FROM {postgres_schema}.{table_name}
            WHERE {incremental_column} > '{last_load_time}'
            ORDER BY {incremental_column}
        """
    else:
        query = f"SELECT * FROM {postgres_schema}.{table_name}"

    connection = pg_hook.get_conn()
    df = pd.read_sql(query, connection)
    connection.close()

    parquet_path = f'/tmp/{table_name}_{context["ds"]}.parquet'
    df.to_parquet(parquet_path, index=False)

    if load_type == 'incremental' and incremental_column and len(df) > 0:
        max_timestamp = df[incremental_column].max()
        ti = context['ti']
        ti.xcom_push(key=f'{table_name}_last_load_time', value=str(max_timestamp))

    return parquet_path


def load_to_snowflake(
    table_name: str,
    parquet_path: str,
    snowflake_database: str,
    snowflake_schema: str,
    load_type: str,
    primary_key: str,
    snowflake_conn_id: str = 'snowflake_default',
    **context
) -> None:

    logger.info(f"Loading data to {snowflake_database}.{snowflake_schema}.{table_name}")

    df = pd.read_parquet(parquet_path)
    if len(df) == 0:
        return

    sf_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    staging_table = f"{table_name}_raw"
    columns = df.columns.tolist()

    column_definitions = []
    for col in columns:
        dtype = df[col].dtype
        if dtype == 'object':
            col_type = 'VARCHAR(16777216)'
        elif dtype == 'int64':
            col_type = 'NUMBER(38,0)'
        elif dtype == 'float64':
            col_type = 'FLOAT'
        elif dtype == 'datetime64[ns]':
            col_type = 'TIMESTAMP_NTZ'
        elif dtype == 'bool':
            col_type = 'BOOLEAN'
        else:
            col_type = 'VARCHAR(16777216)'
        column_definitions.append(f"{col} {col_type}")

    sf_hook.run(f"""
    CREATE TABLE IF NOT EXISTS {snowflake_database}.{snowflake_schema}.{table_name} (
        {', '.join(column_definitions)}
    )
    """)

    sf_hook.run(f"""
    CREATE OR REPLACE TABLE {snowflake_database}.{snowflake_schema}.{staging_table} (
        {', '.join(column_definitions)}
    )
    """)

    sf_hook.run("""
    CREATE FILE FORMAT IF NOT EXISTS MICROHOUSE_RAW.PUBLIC.parquet_format
    TYPE = PARQUET
    """)

    sf_hook.run("""
        REMOVE @MICROHOUSE_RAW.PUBLIC.RAW_STAGE;
    """)

    sf_hook.run(f"""
    PUT file://{parquet_path}
    @MICROHOUSE_RAW.PUBLIC.RAW_STAGE
    AUTO_COMPRESS = FALSE
    """)

    sf_hook.run(f"""
    COPY INTO {snowflake_database}.{snowflake_schema}.{staging_table}
    FROM @MICROHOUSE_RAW.PUBLIC.RAW_STAGE
    FILE_FORMAT = (FORMAT_NAME = MICROHOUSE_RAW.PUBLIC.parquet_format)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'CONTINUE'
    PURGE = TRUE
    """)

    if load_type == 'full':
        sf_hook.run(f"TRUNCATE TABLE {snowflake_database}.{snowflake_schema}.{table_name}")
        sf_hook.run(f"""
        INSERT INTO {snowflake_database}.{snowflake_schema}.{table_name}
        SELECT * FROM {staging_table}
        """)
    else:
        update_cols = [f"target.{c} = source.{c}" for c in columns if c != primary_key]
        insert_cols = ', '.join(columns)
        insert_vals = ', '.join([f"source.{c}" for c in columns])

        sf_hook.run(f"""
        MERGE INTO {snowflake_database}.{snowflake_schema}.{table_name} target
        USING {staging_table} source
        ON target.{primary_key} = source.{primary_key}
        WHEN MATCHED THEN UPDATE SET {', '.join(update_cols)}
        WHEN NOT MATCHED THEN
        INSERT ({insert_cols}) VALUES ({insert_vals})
        """)


def create_transfer_task_group(table_config: Dict) -> TaskGroup:
    table_name = table_config['table_name']

    with TaskGroup(group_id=f'transfer_{table_name}') as group:

        extract_task = PythonOperator(
            task_id=f'extract_{table_name}',
            python_callable=extract_from_postgres,
            op_kwargs=table_config,
        )

        load_task = PythonOperator(
            task_id=f'load_{table_name}',
            python_callable=load_to_snowflake,
            op_kwargs={
                **table_config,
                'parquet_path': f'/tmp/{table_name}_{{{{ ds }}}}.parquet'
            },
        )

        extract_task >> load_task

    return group


with DAG(
    'postgres_loans_to_snowflake',
    default_args=default_args,
    description='Transfer data from PostgreSQL to Snowflake',
    catchup=False,
    tags=['postgres', 'snowflake', 'elt'],
) as dag:

    for table_config in tables_config:
        create_transfer_task_group(table_config)

