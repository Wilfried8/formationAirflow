from airflow.decorators import dag, task
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from pendulum import datetime
import random


with DAG (
    dag_id="test_branch",
    start_date=datetime(2024, 1, 1),
    schedule="0 12 * * *",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"]
) as dag:
    
    start = EmptyOperator(task_id="start")

    @task.branch(task_id = "branch_task")
    def choose_branch():
        random_num = random.randint(1, 20)
        print(f"Random number is : {random_num}")
        
        if random_num % 2 == 0:
            return "even_task"
        else:
            return "odd_task"
    
    branch = choose_branch()

    @task(task_id="even_task")
    def even_task(ti=None):
        
        print("The number is even!")
        

    @task(task_id="odd_task")
    def odd_task():
        
        print("The number is odd!")
        

    end = EmptyOperator(task_id="end")

    start >> branch
    branch >> [even_task(), odd_task()] >> end