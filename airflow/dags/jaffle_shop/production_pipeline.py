import os
from datetime import datetime
from airflow.sdk import dag, task
from airflow.models import DagRun
from airflow.providers.standard.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
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

@dag(
    schedule="@daily",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["learning", "production"],
)

def production_pipeline():

    extract = BashOperator(
        task_id="extract_data",
        bash_command="echo 'Извлекаем новые данные из источника...' && sleep 2 && echo 'Данные получены.'",
    )

    dbt_build = DbtTaskGroup(
        group_id="dbt_build",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=ExecutionConfig(),
        render_config=None,
        operator_args={"install_deps": True},
    )

    @task(trigger_rule="all_success")
    def notify_success():
        print("✅ Пайплайн успешно завершён. Все данные обновлены.")

    @task(trigger_rule="one_failed")
    def notify_failure():
        print("🚨 Пайплайн завершился с ошибками. Проверьте dbt_build.")

    extract >> dbt_build >> [notify_success(), notify_failure()]    

production_pipeline()