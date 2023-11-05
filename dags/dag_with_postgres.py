from datetime import datetime, timedelta

from airflow import DAG

from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "Luciano",
    "start_date": datetime(2023, 11, 4),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="DagTestPostgres04",
    description="My first DAG",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:
    CreateTable = PostgresOperator(
        task_id="CreateTable",
        postgres_conn_id="psotgres-workshop",
        sql="""
        CREATE TABLE IF NOT EXISTS public.user (
            id serial PRIMARY KEY,
            name VARCHAR ( 50 ) NOT NULL,
            age INT NOT NULL
        );
        """,
    )

    InserIntoTable = PostgresOperator(
        task_id="InserIntoTable",
        postgres_conn_id="psotgres-workshop",
        sql="""
        INSERT INTO public.user (name, age) VALUES ('Luciano', 30);
        """,
    )

    InserIntoTable02 = PostgresOperator(
        task_id="InserIntoTable02",
        postgres_conn_id="psotgres-workshop",
        sql="""
        INSERT INTO public.user (name, age) VALUES ('Gustavo', 30);
        """,
    )

    CreateTable >> InserIntoTable >> InserIntoTable02
