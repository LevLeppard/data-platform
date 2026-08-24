from airflow.sdk import dag, task
from datetime import datetime, timedelta
import random

def notify_failure(context):
    task_id = context.get("task_instance").task_id
    print(f"🚨 ALERT: таск '{task_id}' провалился окончательно! (тут был бы Slack/email)")

@dag(
    schedule=None,
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["learning", "reliability"],
)

def retry_alert_demo():

    @task(
        retries=3,
        retry_delay=timedelta(seconds=10),
        on_failure_callback=notify_failure,
    )
    def flaky_task():
        success = random.choice([True, False, False])
        print(F"Попытка выполнения... успех: {success}")
        if not success:
            raise Exception("Симулированная ошибка сети")
        print("Задача выполнена успешно!")
    
    flaky_task()

retry_alert_demo()