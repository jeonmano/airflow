
from datetime import datetime, timedelta
from airflow.decorators import dag, task
@dag(
    dag_id='test_v2',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def dag_name():

    @task
    def hello():
        print(f"hello airflow")
    hello()
    
dag_name()
