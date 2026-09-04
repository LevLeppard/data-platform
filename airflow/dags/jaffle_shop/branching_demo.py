from airflow.sdk import dag, task
from datetime import datetime
import random

@dag(
    schedule=None,
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["learning", "branching"],
)
def branching_demo():

    @task
    def check_new_rows():
        rows = random.randint(0, 42)
        print(f"Найдено новых строк: {rows}")
        return rows

    @task.branch
    def decide_path(rows):
        if rows > 0:
            return "process_data"
        else:
            return "skip_processing"

    @task
    def process_data():
        print("Обрабатываем данные...")
    
    @task
    def skip_processing():
        print("Нет новых данных для обработки.")
    
    rows = check_new_rows()
    decide_path(rows) >> [process_data(), skip_processing()]
    
branching_demo()
    