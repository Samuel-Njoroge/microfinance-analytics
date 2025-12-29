# MicroFinance Analytics Platform

## 1. Introduction

This repository contains the **end-to-end analytics and data platform** for a **Microfinance Institution**, designed to support operational reporting, financial analytics, customer insights, and executive dashboards.

The platform follows **modern analytics engineering best practices**, separating concerns across **data ingestion**, **transformation**, and **analytics consumption**.

### Key Objectives

* Reliable ingestion from **Django/Postgres OLTP systems**
* Scalable analytics on **Snowflake**
* Modular transformations using **dbt**
* Orchestration and monitoring via **Apache Airflow**
* Self-service BI using **Apache Superset**

---

## 2. High-Level Architecture

![architecture](./superset/assets/architecture.svg)

---

## 3. Tech Stack

| Layer           | Technology      |
| --------------- | --------------- |
| Backend         | Django          |
| OLTP Database   | PostgreSQL      |
| Orchestration   | Apache Airflow  |
| Data Warehouse  | Snowflake       |
| Transformations | dbt             |
| BI / Analytics  | Apache Superset |
| Language        | Python, SQL     |

---

## 4. Repository Structure

```
.
├── airflow/
│   ├── dags/
│   │   ├── postgres_accounts_to_snowflake.py
│   │   ├── postgres_investments_to_snowflake.py
│   │   ├── postgres_loans_to_snowflake.py
│   │   ├── postgres_payments_to_snowflake.py
│   │   ├── postgres_transactions_to_snowflake.py
│   │   └── postgres_users_to_snowflake.py
│   │── plugins/
│   └── docker-compose.yml
│
├── dbt/finhouse
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       ├── intermediate/
│       └── marts/
│
├── snowflake/
│   ├── assets/
│   ├── scripts/
│   └── README.md
│
├── superset/
│   ├── assets/
│   └── dashboards/
│
└── README.md
```

---

## 5. Data Flow Overview

### Step 1: Source System (OLTP)

* Django backend writes transactional data to **Postgres**
* Highly normalized, write-optimized schema

---

### Step 2: Extraction & Loading (Airflow)

* Airflow DAGs:

  * Extract data from Postgres
  * Write to Snowflake **RAW (bronze) layer**
* Supports:

  * Full loads
  * Incremental loads
* Raw data remains **unaltered**

---

### Step 3: Transformations (dbt)

dbt models are organized into **three layers**:

#### 3.1 Staging Layer

* Light transformations
* Renaming
* Type casting
* Deduplication
* One-to-one mapping with source tables

#### 3.2 Intermediate Layer

* Business logic
* Joins across entities
* Derived metrics
* Slowly changing calculations

#### 3.3 Marts Layer

* Analytics-ready tables
* Aggregations
* KPIs
* Optimized for BI consumption

---

### Step 4: Analytics & BI (Superset)

* Snowflake marts exposed as datasets
* Interactive dashboards for:

  * Customer analytics
  * Loan performance
  * Investment returns
  * Financial health metrics

---

## 6. dbt Project Configuration

### Schema Strategy

| Layer        | Snowflake Schema |
| ------------ | ---------------- |
| Staging      | `STAGING`        |
| Intermediate | `INTERMEDIATE`   |
| Marts        | `MARTS`          |

Configured in `dbt_project.yml`:

```yaml
models:
  finhouse:
    staging:
      +schema: staging
    intermediate:
      +schema: intermediate
    marts:
      +schema: marts
```

## 7. Data Quality & Testing

dbt tests include:

* `not_null`
* `unique`
* `accepted_values`
* Relationship tests

Executed automatically in Airflow pipelines.

---

## 8. Future Enhancements

* CDC via Debezium
* Data freshness SLAs
* Column-level lineage
* Feature store for ML
* Real-time dashboards

---

## 9. Contributing

1. Create feature branch
2. Add dbt models with tests
3. Validate locally
4. Submit PR

