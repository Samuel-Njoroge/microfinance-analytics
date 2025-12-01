# Analytics Architecture

                         ┌───────────────────────────┐
                         │   Django Backend (OLTP)    │
                         │    + Postgres database     │
                         └───────────────┬────────────┘
                                         │
                                 Airflow Orchestrates
                                         │
                                         ▼
                       ┌──────────────────────────────────┐
                       │   Airflow Extract DAGs           │
                       │   - Read from Postgres OLTP      │
                       │   - Load into Snowflake RAW       │
                       └──────────────────┬────────────────┘
                                          │
                                          ▼
                           Snowflake RAW ("bronze") Layer
                                          │
                                          ▼
                       ┌──────────────────────────────────┐
                       │       dbt Transformations        │
                       │ staging → intermediate → marts   │
                       └──────────────────┬────────────────┘
                                          │
                                          ▼
                        Snowflake Marts (“gold”) Analytics Layer
                                          │
                                          ▼
                ┌────────────────────────────────────────────────┐
                │ BI + Reporting: Apache Superset                │
                │ - Dashboards                                   │
                │ - Metrics / Data exploration                   │
                └────────────────────────────────────────────────┘

                               (Optional)
                                     │
                                     ▼
                      ML models / Feature engineering workflows
                               orchestrated via Airflow

project-root/
│
├── backend/                         # Django Application
│   ├── manage.py
│   ├── requirements.txt
│   ├── project_name/
│   ├── apps/
│   └── ...
│
├── data-platform/
│   │
│   ├── airflow/                     # Airflow orchestration
│   │   ├── docker-compose.yml
│   │   ├── .env
│   │   ├── dags/
│   │   │   ├── extract/
│   │   │   │   ├── extract_postgres_to_snowflake.py
│   │   │   ├── transform/
│   │   │   │   ├── dbt_run.py
│   │   │   └── utils/
│   │   ├── logs/
│   │   └── plugins/
│   │
│   ├── dbt/                         # dbt project
│   │   ├── dbt_project.yml
│   │   ├── models/
│   │   │   ├── staging/
│   │   │   ├── intermediate/
│   │   │   ├── marts/
│   │   │   └── macros/
│   │   ├── seeds/
│   │   ├── snapshots/
│   │   └── profiles/
│   │
│   ├── warehouse/                   # Warehouse configs
│   │   ├── snowflake/
│   │   │   ├── init.sql             # Schema + roles + grants
│   │   │   ├── raw_schema.sql
│   │   │   ├── staging_schema.sql
│   │   │   └── marts_schema.sql
│   │
│   └── superset/                    # Superset deployment
│       ├── docker-compose.yml
│       ├── superset_config.py
│       └── dashboards/              # Exported dashboards
│
├── scripts/                         # Utility scripts
│   ├── local_ingest.py
│   ├── setup_airflow.sh
│   └── db_setup.sql
│
└── docs/                            # Architecture & docs
    ├── data-model.md
    ├── airflow-dags.md
    └── warehouse.md
