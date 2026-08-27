# Data Platform: dbt + Airflow

A learning pet project implementing a full ELT pipeline: data transformation with dbt, orchestration and monitoring through Apache Airflow. Built on the classic Jaffle Shop dataset.

## Architecture

```
Postgres (dbt-postgres, Docker)
│
├── raw_customers / raw_orders / raw_payments        seeds (learning stand-in for a source)
│
├── staging/                                          1:1 with the raw data
│   ├── stg_customers
│   ├── stg_orders
│   └── stg_payments        (+ cents_to_dollars macro)
│
├── intermediate/
│   └── int_customer_orders  (join stg_orders + stg_customers)
│
├── marts/                                            analytics data marts
│   ├── dim_customers
│   └── fct_orders           (incremental)
│
└── snapshots/
    └── orders_snapshot      (SCD Type 2, order change history)
```

Orchestration in Airflow (`airflow/dags/production_pipeline.py`):

```
extract_data (BashOperator)
      │
      ▼
dbt_build (Cosmos DbtTaskGroup — 16 granular tasks: seed/run/test/snapshot)
      │
      ├──► notify_success   (trigger_rule=all_success)
      └──► notify_failure   (trigger_rule=one_failed)
```

## Stack

* dbt-core 1.12 + `dbt-postgres`
* Apache Airflow (Astro Runtime, Airflow 3.x)
* astronomer-cosmos — integrates the dbt project as native Airflow tasks
* Postgres — data warehouse and Airflow metadata (separate containers)
* Docker / Astro CLI — local environment

## Repository structure

```
data-platform/
├── dbt/
│   └── jaffle_shop/           # dbt project
│       ├── models/
│       ├── seeds/
│       ├── snapshots/
│       └── profiles.yml       # NOT in git, created manually (see below)
├── airflow/
│   ├── dags/
│   │   ├── production_pipeline.py   # main pipeline
│   │   ├── dbt_cosmos_demo.py       # Cosmos demo: the dbt project as a whole
│   │   └── examples/                # learning DAGs (branching, retries, XCom, etc.)
│   ├── requirements.txt
│   └── docker-compose.override.yml  # volume mapping of the dbt project into the container
└── docker-compose.yml         # Postgres for dbt
```

## Getting started from scratch

1. Postgres for dbt:

```bash
docker compose up -d
```

2. dbt environment:

```bash
python3 -m venv ~/.venvs/data-platform
source ~/.venvs/data-platform/bin/activate
pip install dbt-postgres
```

3. `~/.dbt/profiles.yml` (for running dbt from the host):

```yaml
jaffle_shop:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: dbtuser
      password: dbtpass
      dbname: dbtdb
      schema: schema
      threads: 4
```

4. `dbt/jaffle_shop/profiles.yml` (for running from the Airflow container — the same config, but `host: host.docker.internal`)

5. Populate and verify the dbt project:

```bash
cd dbt/jaffle_shop
dbt build
```

6. Airflow:

```bash
cd airflow
astro dev start
```

UI: http://localhost:8080

7. Airflow connection `dbt_postgres` (Admin → Connections): host `host.docker.internal`, port `5432`, login/password/schema as above — used by Cosmos to dynamically generate the dbt profile.

## What's implemented

* A full layered dbt project: staging → intermediate → marts, with data quality tests (`unique`, `not_null`, `accepted_values`)
* Incremental materialization (`fct_orders`)
* Snapshot (SCD Type 2) for order change history
* A custom macro (`cents_to_dollars`) and an external package (`dbt_utils`)
* Two ways to integrate with Airflow: `BashOperator` (simple) and `astronomer-cosmos` (granular, one task per model)
* A production-like DAG with conditional success/failure notifications via `trigger_rule`
* Retry logic and an `on_failure_callback` at the task level

## Author

Learning project, completed following a 14-day dbt + Airflow study plan.