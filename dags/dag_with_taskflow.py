from airflow.decorators import dag, task

from datetime import datetime, timedelta

default_args = {
    'owner': 'luciano',
    'start_date': datetime(2023, 11, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(dag_id='newdag_v7', 
     description='My first DAG', 
     schedule_interval='0 0 * * *', 
     default_args=default_args, 
     catchup=False)

def hello_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {'first_name': 'Luciano', 'last_name': 'Filho'}
    
    @task()
    def get_age():
        return 30
    
    @task()
    def greet(first_name, last_name, age):
        print(f'Hello {first_name} {last_name}! You are {age} years old.')

    name_full = get_name()
    age = get_age()
    greet(first_name = name_full['first_name'],
          last_name = name_full['last_name'],
          age = age)

dag = hello_etl()