from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk import Variable
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

def rest_api_petroleum_input_utilization(**context):
    api_url = Variable.get("eia_api_url")
    api_key = Variable.get("eia_api_key")

    route = "petroleum/pnp/wiup/data/"
    url = f"{api_url}/{route}"

    params = {
        "api_key": api_key,
        "frequency": "weekly",
        "data[0]": "value",
        "start": "2020-01-01",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 50,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()["response"]["data"]
    df = pd.DataFrame(data)
    df["loaded_at"] = datetime.utcnow()
    hook = PostgresHook(postgres_conn_id="dbt-postgres-eia")
    engine = hook.get_sqlalchemy_engine()

    df.to_sql(
        "raw_petroleum_input_utilization",
        engine,
        schema="raw",
        if_exists="append",
        index=False,
    )

with DAG(
    dag_id="rest_api_petroleum_input_utilization",
    default_args=default_args,
    schedule="@weekly",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["eia", "petroleum", "input_utilization"],
) as dag:

    extract_load = PythonOperator(
        task_id="extract_load_petroleum_input_utilization",
        python_callable=rest_api_petroleum_input_utilization,
    )
