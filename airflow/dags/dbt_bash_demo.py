from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

DBT_PROJECT_DIR = "/usr/local/airflow/dbt/jaffle_shop"

@dag(
    schedule=None,
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["learning", "dbt"],
)

def dbt_bash_demo():

    DBT_FLAGS = "--profiles-dir . --log-path /tmp/dbt_logs --target-path /tmp/dbt_target"

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run {DBT_FLAGS}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test {DBT_FLAGS}",
    )

    dbt_run >> dbt_test

dbt_bash_demo()