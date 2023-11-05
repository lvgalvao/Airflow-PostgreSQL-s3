from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import csv
import os

import logging

# Constants
FILENAME = "/tmp/new_data.csv"
SQL_QUERY = "SELECT * FROM public.orders;"
BUCKET_NAME = "airflow"
S3_KEY = "new_data.csv"

default_args = {
    "owner": "Luciano",
    "start_date": datetime(2023, 11, 4),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def extract_postgres_data(**kwargs):
    hook = PostgresHook(postgres_conn_id="psotgres-workshop")
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(SQL_QUERY)
            with open(FILENAME, "w") as f:
                csv_writer = csv.writer(f)
                csv_writer.writerow(
                    [i[0] for i in cursor.description]
                )  # column headers
                csv_writer.writerows(cursor)
            logging.info("Data extracted from Postgres and saved to CSV.")


def load_file_to_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id="minio_s3_io")
    if os.path.isfile(FILENAME):
        s3_hook.load_file(
            filename=FILENAME,
            key=S3_KEY,
            bucket_name=BUCKET_NAME,
            replace=True,
        )
        logging.info("File uploaded to S3.")
    else:
        raise FileNotFoundError(f"{FILENAME} does not exist.")


with DAG(
    dag_id="PostgresToS3_v3",
    description="A Pipeline to pull data from Postgres and store it on S3",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:
    task_extract_postgres_data = PythonOperator(
        task_id="extract_postgres_data",
        python_callable=extract_postgres_data,
    )

    task_load_file_to_s3 = PythonOperator(
        task_id="load_file_to_s3",
        python_callable=load_file_to_s3,
    )

    task_extract_postgres_data >> task_load_file_to_s3
