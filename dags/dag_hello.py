from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'luciano',
    'start_date': '2023-11-02',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG (
    dag_id = 'newdag',
    description='My first DAG',
    schedule_interval = '0 0 * * *',
    default_args = default_args,
    catchup = False
) as dag:
    task1 = BashOperator(
        task_id = 'task1',
        bash_command = 'echo "Hello World!"'
    )
    
    task1
