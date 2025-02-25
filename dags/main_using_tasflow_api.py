from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.decorators import task, task_group
from airflow.models import Variable
from datetime import datetime
import random
from airflow.utils.task_group import TaskGroup

# Defininf Airflow variables
default_warehouse = Variable.get("default_warehouse", default_var="WH001")
threshold_stock = int(Variable.get("threshold_stock", default_var="10")) # Seuil d'alerte stock

# Definition of DAG
with DAG(
    dag_id="ecommerce_order_processing_with_task_decorator",
    schedule_interval="@daily",
    start_date=datetime(2025, 2, 24),
    catchup=False # to avoid running past runs.
) as dag:
    
    @task
    def check_variables():
        print(f"warehouse: {default_warehouse}, stock Threshold: {threshold_stock} ")
    
    @task.branch
    def check_stock():
        current_stock = random.randint(0, 20)  
        print(f"Current stock in {default_warehouse}: {current_stock}")
        
        if current_stock >= threshold_stock:
            return "order_processing.process_order"
        else:
            return "order_processing.reject_order"
        
    @task_group( group_id="order_processing")
    def order_processing_group():
        @task
        def process_order():
            print("✅ The order is validated and being processed.")
        
        @task
        def reject_order():
            print("❌ The order is rejected, insufficient stock.")
            
        return process_order(), reject_order()
            
    process_order_task, reject_order_task = order_processing_group()

    # DAG dependencies (managed automatically by TaskFlow API)
    variable = check_variables()
    stock_decision = check_stock()

    variable >> stock_decision 
    stock_decision >> [process_order_task, reject_order_task]