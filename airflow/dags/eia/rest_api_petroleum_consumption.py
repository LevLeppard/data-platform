from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk import Variable
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2
from sqlalchemy import text
from airflow.providers.postgres.hooks.postgres import PostgresHook
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.constants import ExecutionMode
from cosmos import RenderConfig

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

def rest_api_petroleum_consumption(**context):
    api_url = "https://api.eia.gov/v2"
    api_key = Variable.get("eia_api_key")

    route = "petroleum/cons/wpsup/data/"
    url = f"{api_url}/{route}"

    hook = PostgresHook(postgres_conn_id="dbt-postgres-eia")
    engine = hook.get_sqlalchemy_engine()

    with engine.connect() as conn:
        try:
            result = conn.execute(text("SELECT MAX(period::date) FROM raw.raw_petroleum_consumption")).fetchone()
            last_period = result[0] if result and result[0] else None
        except Exception:
            last_period = None

    params_start = last_period.strftime("%Y-%m-%d") if last_period else "2000-01-01"
    
    params = {
        "api_key": api_key,
        "frequency": "weekly",
        "data[0]": "value",
        "start": params_start,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 5000,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()["response"]["data"]
    df = pd.DataFrame(data)
    df["loaded_at"] = datetime.utcnow()
    hook = PostgresHook(postgres_conn_id="dbt-postgres-eia")
    engine = hook.get_sqlalchemy_engine()

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.commit()

    df.to_sql(
        "raw_petroleum_consumption",
        engine,
        schema="raw",
        if_exists="append",
        index=False,
    )

profile_config = ProfileConfig(
    profile_name="eia",
    target_name="dev",
    profiles_yml_filepath="/usr/local/airflow/dbt/eia/profiles.yml",
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
)


with DAG(
    dag_id="rest_api_petroleum_consumption",
    default_args=default_args,
    schedule="@weekly",
    start_date=datetime(2000, 1, 1),
    catchup=False,
    tags=["eia", "petroleum", "consumption", "dbt"],
) as dag:

    extract_load = PythonOperator(
        task_id="extract_load_petroleum_consumption",
        python_callable=rest_api_petroleum_consumption,
    )

    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform_petroleum_consumption",
        project_config=ProjectConfig(
            "/usr/local/airflow/dbt/eia",
        ),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["stg_petroleum_consumption+"],
        ),
    )

    extract_load >> dbt_transform
