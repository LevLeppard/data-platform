from airflow.sdk import dag, task
from datetime import datetime

@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["learning"],
)
def hello_dbt_platform():
    @task()
    def say_hello():
        print("Hello from Airflow, скоро здесь будет dbt run.")

    say_hello()
hello_dbt_platform()