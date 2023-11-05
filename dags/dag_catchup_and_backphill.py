from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'luciano',
    'start_date': datetime(2023, 10, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG (
    dag_id = 'dag_with_catchup_and_backphill_v5',
    description='My first DAG',
    schedule_interval = '* * * * *',
    default_args = default_args,
    catchup = False
) as dag:
    task1 = BashOperator(
        task_id = 'task1',
        bash_command = 'echo "Hello World!"'
    )
