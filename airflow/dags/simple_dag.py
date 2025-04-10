import time
import random
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.operators.bash import BashOperator

def generate_and_print_data():
    start_time = time.time()
    while time.time() - start_time < 300:
        data = random.randint(1, 100)
        print(f"Generated data: {data}")
        time.sleep(1) 



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 12),
    'retries': 1,
}

dag = DAG(
    'simple_data_generation',
    default_args=default_args,
    description='A simple DAG that generates and prints data for 30 seconds',
    schedule_interval=None, 
    catchup=False,
)

display_message = BashOperator(
    task_id='display_terminal_message',
    bash_command='echo START ... ONCE UPON A TIME | figlet'
)

generate_and_print_task = PythonOperator(
    task_id='generate_and_print_data',
    python_callable=generate_and_print_data,
    dag=dag,
)

display_message >> generate_and_print_task
