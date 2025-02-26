from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.decorators import task, task_group
from airflow.models import Variable
from datetime import datetime
import random
import os

# Defining Airflow variables
default_warehouse = Variable.get("default_warehouse", default_var="WH001")
threshold_stock = int(Variable.get("threshold_stock", default_var="10"))

# Path to SQL files
SQL_PATH = os.path.join(os.path.dirname(__file__), "sql/")

# Definition of DAG
with DAG(
    dag_id="ecommerce_order_processing_with_sql_files",
    schedule_interval="@daily",
    start_date=datetime(2025, 2, 24),
    catchup=False  
) as dag:
    
    @task
    def check_variables():
        print(f"Warehouse: {default_warehouse}, Stock Threshold: {threshold_stock}")

    @task.branch
    def check_stock():
        current_stock = random.randint(0, 20)  
        print(f"Current stock in {default_warehouse}: {current_stock}")
        
        if current_stock >= threshold_stock:
            return "order_processing.process_order"
        else:
            return "order_processing.reject_order"
        
    @task_group(group_id="order_processing")
    def order_processing_group():
        @task
        def process_order():
            print("✅ The order is validated and being processed.")
        
        @task
        def reject_order():
            print("❌ The order is rejected, insufficient stock.")
            
        return process_order(), reject_order()
    
    
    # Task to create a local SQLite table
    create_table = SqliteOperator(
        task_id="create_stock_table",
        sqlite_conn_id="sqlite_default",
        sql="sql/create_stock_table.sql"
        
    )
    
    check_tables = SqliteOperator(
    task_id="check_tables",
    sqlite_conn_id="sqlite_default",
    sql="SELECT name FROM sqlite_master WHERE type='table';"
)
    
    # Task to insert test data
    insert_data = SqliteOperator(
        task_id="insert_test_data",
        sqlite_conn_id="sqlite_default",
        sql="sql/insert_test_data.sql",
    )

    # Task to check stock using SQLite
    check_stock_sqlite = SqliteOperator(
        task_id="check_stock_sqlite",
        sqlite_conn_id="sqlite_default",
        sql="sql/check_stock.sql",
    )
    
    process_order_task, reject_order_task = order_processing_group()
    
    # DAG dependencies
    variable = check_variables()
    stock_decision = check_stock()
    
    variable >> create_table
    
    create_table >> check_tables >> insert_data >> stock_decision
    stock_decision >> check_stock_sqlite >> [process_order_task, reject_order_task]