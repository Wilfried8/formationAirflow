from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime

# Defininf Airflow variables
default_warehouse = Variable.get("default_warehouse", default_var="WH001")
threshold_stock = Variable.get("threshold_stock", default_var="10") # Seuil d'alerte stock

# Function to display variables

def print_variables():
    print(f"default warehouse : {default_warehouse}")
    print(f"threshold stock : {threshold_stock}")
    
# Definition of DAG
with DAG(
    dag_id="ecommerce_order_processing",
    schedule_interval="@daily",
    start_date=datetime(2025, 2, 24),
    catchup=False # to avoid running past runs.
) as dag:
    
    #@task()
    check_variable = PythonOperator(
        task_id="check_variables",
        python_callable=print_variables,
    )
    
    check_variable
