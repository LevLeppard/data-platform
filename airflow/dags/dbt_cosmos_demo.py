import os
from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_PROJECT_DIR = "/usr/local/airflow/dbt/jaffle_shop"

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="dbt_postgres",
        profile_args={"schema": "schema"},
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt"
    if os.path.exists("/usr/local/airflow/dbt_venv/bin/dbt") 
    else "dbt",
)

dbt_cosmos_demo = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_DIR),
    profile_config=profile_config,
    execution_config=execution_config,
    schedule=None,
    start_date=datetime(2026, 8, 1),
    catchup=False,
    dag_id="dbt_cosmos_demo",
    tags=["learning", "dbt", "cosmos"],
)