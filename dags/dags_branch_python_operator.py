from airflow import DAG
from datetime import datetime
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator


with DAG(
    dag_id='dags_branch_python_operator',
    start_date=datetime(2023,2,1),
    schedule=None,
    catchup=False
) as dag:

    def select_random():
        import random

        item_list = ['A','B','C']
        selected_time = random.choice(item_list)
        if selected_time == 'A':
            return 'task_a'
        elif selected_time in ['B','C"']:
            return ['task_b','task_c']
        
    python_branch_task = BranchPythonOperator(
        task_id='python_branch_task',
        python_callable=select_random
    )

    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )


python_branch_task >> [task_a,task_b,task_c]