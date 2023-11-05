from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator


def pandas_init():
    import pandas

    print(f"Pandas version: {pandas.__version__}")


default_args = {
    "owner": "Luciano",
    "start_date": datetime(2023, 11, 2),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="my_first_pandas_dag",
    description="My first DAG",
    schedule_interval="0 0 * * *",
    default_args=default_args,
    catchup=False,
) as dag:
    pandas_version = PythonOperator(
        task_id="pandas_version", python_callable=pandas_init
    )

    pandas_version
