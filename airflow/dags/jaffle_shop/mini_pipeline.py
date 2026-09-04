from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

@dag(
    schedule="@daily",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["learning", "pipeline"]
)
def mini_pipeline():

    extract = BashOperator(
        task_id="extract",
        bash_command="echo 'Extracting data from source...' && sleep 2",
    )

    @task()
    def transform():
        print("Transforming data...")
        return {"rows_processed": 99}

    @task()
    def validate(stats):
        rows = stats["rows_processed"]
        print(f"Validating {rows} rows...")
        if rows == 0:
            raise ValueError("No rows to validate!")
        print("Validation passed.")

    notify = BashOperator(
        task_id="notify",
        bash_command="echo 'Pipeline finished successfully!'",
    )

    transformed = transform()
    extract >> transformed >> validate(transformed) >> notify

mini_pipeline()