from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


@dag(
    dag_id="select",
    schedule=None,
    start_date=datetime(2026, 1, 24), # 오늘부터 시작
    catchup=False,
)
def select():
    @task
    def select_query():
        pg_hook = PostgresHook(postgres_conn_id='airflow_db_wsl')
        query = 'select * from test'
        result = pg_hook.get_records(query)
        print(f"결과 값은 {result}")
    select_query()
select()
        




