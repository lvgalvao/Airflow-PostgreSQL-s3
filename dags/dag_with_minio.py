from datetime import datetime, timedelta

from airflow import DAG

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


default_args = {
    "owner": "Luciano",
    "start_date": datetime(2023, 11, 2),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="my_first_dag_using_minio_v02",
    description="My first DAG",
    schedule_interval="0 0 * * *",
    default_args=default_args,
    catchup=False,
) as dag:
    wait_for_file = S3KeySensor(
        task_id="wait_for_file",
        bucket_name="airflow",
        bucket_key="data.csv",
        aws_conn_id="minio_s3_io",
        mode="poke",
        poke_interval=5,
        timeout=30,
    )

    wait_for_file
