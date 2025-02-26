from airflow import DAG
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task, task_group
from airflow.models import Variable
from datetime import datetime
import os

# Airflow variable configuration
default_warehouse = Variable.get("default_warehouse", default_var="WH001")
threshold_stock = int(Variable.get("threshold_stock", default_var="10"))

# SQL file path definition
SQL_PATH = os.path.join(os.path.dirname(__file__), "sql/")

# DAG
with DAG(
    dag_id="ecommerce_order_processing_with_sql_files_xcom",
    schedule_interval="@daily",
    start_date=datetime(2025, 2, 24),
    catchup=False  
) as dag:
    
    # Create stock table task
    create_table = SqliteOperator(
        task_id="create_stock_table",
        sqlite_conn_id="sqlite_default",
        sql="sql/create_stock_table.sql"
    )

    # Test data insertion task
    insert_data = SqliteOperator(
        task_id="insert_test_data",
        sqlite_conn_id="sqlite_default",
        sql="sql/insert_test_data.sql",
    )

    # Check stock task for specified warehouse
    check_stock_sqlite = SqliteOperator(
        task_id="check_stock_sqlite",
        sqlite_conn_id="sqlite_default",
        sql="sql/check_stock.sql",
        do_xcom_push=True # Send the result of query to XCOM
    )
    
    @task.branch
    def check_stock(ti):
        """Read the stock from XCom and select the next branch."""
        current_stock = ti.xcom_pull(task_ids="check_stock_sqlite")
        
        if not current_stock:
            print("⚠️ No stock data found! Skipping order processing.")
            return "fallback_task"

        stock_level = current_stock[0][0]  # Retrieve the value of the SQL query from XCOM
        print(f"Current stock in {default_warehouse}: {stock_level}")

        return "order_processing.process_order" if stock_level >= threshold_stock else "order_processing.reject_order"

    branch_decision = check_stock()

    # Task groups for order processing
    @task_group(group_id="order_processing")
    def order_processing_group():
        @task
        def process_order():
            print("✅ The order is validated and being processed.")

        @task
        def reject_order():
            print("❌ The order is rejected due to insufficient stock.")

        return process_order(), reject_order()

    # Backup task in case of problems with decision making
    fallback_task = EmptyOperator(task_id="fallback_task")

    # Definition of DAG dependencies
    create_table >> insert_data >> check_stock_sqlite >> branch_decision
    process_order_task, reject_order_task = order_processing_group()
    branch_decision >> [process_order_task, reject_order_task, fallback_task]
