import time
import random
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def generate_and_print_data():
    start_time = time.time()
    while time.time() - start_time < 30:
        data = random.randint(1, 100)
        print(f"Generated data: {data}")
        time.sleep(1)  # Print data every second




# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 12),
    'retries': 1,
}

# Instantiate the DAG
dag = DAG(
    'simple_data_generation',
    default_args=default_args,
    description='A simple DAG that generates and prints data for 30 seconds',
    schedule_interval=None,  # Set to None to run manually or use a cron expression
    catchup=False,
)

# Define the Python task
generate_and_print_task = PythonOperator(
    task_id='generate_and_print_data',
    python_callable=generate_and_print_data,
    dag=dag,
)

# Set the task dependencies
generate_and_print_task
