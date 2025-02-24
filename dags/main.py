from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime
import random

# Defininf Airflow variables
default_warehouse = Variable.get("default_warehouse", default_var="WH001")
threshold_stock = int(Variable.get("threshold_stock", default_var="10")) # Seuil d'alerte stock

# Function to display variables
def print_variables():
    print(f"default warehouse : {default_warehouse}")
    print(f"threshold stock : {threshold_stock}")
    
# Function to check stock and decide on route
def check_stock():
    current_stock = random.randint(0, 25)
    print(f"current stock of {default_warehouse} : {current_stock}")
    
    if current_stock >= threshold_stock:
        return "process_order"
        
    else:
        return "rejected_order"
        
    
# Function for order processing
def process_order():
    print("✅ The order is validated and being processed.")
    
# Function for rejecting the order
def reject_order():
    print("❌ The order is rejected, insufficient stock.")

    
# Definition of DAG
with DAG(
    dag_id="ecommerce_order_processing",
    schedule_interval="@daily",
    start_date=datetime(2025, 2, 24),
    catchup=False # to avoid running past runs.
) as dag:
    
    check_variables = PythonOperator(
        task_id="check_variables",
        python_callable=print_variables,
    )
    
    check_stock_task = BranchPythonOperator(
        task_id="check_stock",
        python_callable=check_stock,
    )

    process_order_task = PythonOperator(
        task_id="process_order",
        python_callable=process_order,
    )

    reject_order_task = PythonOperator(
        task_id="reject_order",
        python_callable=reject_order,
    )
    
    check_variables >> check_stock_task
    check_stock_task >> [process_order_task, reject_order_task]
