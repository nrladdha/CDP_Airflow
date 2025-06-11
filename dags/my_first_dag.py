from __future__ import annotations
from datetime import datetime
from airflow.sdk import DAG, Label
from airflow.operators.python import PythonOperator

def my_function():
    print("Hello from Airflow!")

with DAG(
    "my_first_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily", 
    catchup=False
) as dag:
    hello_task = PythonOperator(
        task_id="print_hello",
        python_callable=my_function,
    )

    hello_task