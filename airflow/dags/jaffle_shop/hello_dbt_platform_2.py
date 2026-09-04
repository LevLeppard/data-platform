from airflow.sdk import dag, task
from datetime import datetime

@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["learning"],
)
def hello_dbt_platform_2():
    @task()
    def say_hello():
        print("Привет от первого такска")
        return "hello_done"
    @task()
    def say_goodbye(previous_result):
        print(f"Второй таск получил: {previous_result}")

    say_goodbye(say_hello())

hello_dbt_platform_2()