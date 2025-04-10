from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
import pendulum
import datetime

with DAG (
    dag_id="dags_python_email_op",
    schedule="30 6 * * * ",
    start_date=pendulum.datetime(2025,4,1,tz="Asia/Seoul"),
    catchup=False
) as dag:
    @task(task_id="choice_task")
    def some_logic(**kwargs):
        from random import choice
        return choice(['Success','Fail'])
    
    send_email = EmailOperator(
        task_id="send_email",
        to='junmh87@naver.com',
        subject='{{data_interval_end.in_timezone("Asia/Seoul") | ds}} some logic 처리 결과',
        html_content='{{data_interval_end.in_timezone("Asia/Seoul") | ds}} 처리 결과는 <br> \
            {{ti.xcom_pull(task_ids="choice_task")}} 했습니다 <br>'
    )

    some_logic() >> send_email