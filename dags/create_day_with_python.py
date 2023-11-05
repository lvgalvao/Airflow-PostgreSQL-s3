from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

def greet(ti):
    first_name = ti.xcom_pull(task_ids=['task2'], key='first_name')
    last_name = ti.xcom_pull(task_ids=['task2'], key='last_name')
    age = ti.xcom_pull(task_ids=['task3'], key='age')
    print(f'Hello {first_name} {last_name}! You are {age} years old.')

def get_name(ti):
    ti.xcom_push(key='first_name', value='Luciano')
    ti.xcom_push(key='last_name', value='Filho')

def get_age(ti):
    ti.xcom_push(key='age', value=30)

default_args = {
    'owner': 'luciano',
    'start_date': datetime(2023, 11, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='my_first_python_dag_v7',
    description='My first DAG',
    schedule_interval='0 0 * * *',
    default_args=default_args,
    catchup=False
) as dag:
    task1 = PythonOperator(
        task_id='task1',
        python_callable=greet,
        op_kwargs={'age': 30}
    )

    task2 = PythonOperator(
        task_id='task2',
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id='task3',
        python_callable=get_age
    )

    [task2, task3] >> task1