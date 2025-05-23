from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
import random
from airflow.decorators import task

with DAG(
    dag_id="dags_python_task_decorator",
    schedule=None,
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    @task(task_id="python_task_1")
    def print_context(some_input):
        print(some_input)

    python_task_1 = print_context('task decorator 실행')