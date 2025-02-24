from airflow import DAG
from airflow.decorators import task
from pendulum import datetime

default_args={
    "owner": "wilfried", 
    "retries": 3 
    }
# Définir le DAG
with DAG(
    dag_id='xcom_example_dag',
    schedule_interval="@daily",
    start_date=datetime(2024, 10, 20),
    default_args=default_args,
    catchup=False,
) as dag:

    # Première tâche: pousser une valeur dans XCom
    @task(task_id="push_value")
    def push_value():
        my_value = "Hello from XCom!"
        # On retourne la valeur pour la pousser dans XCom
        return my_value

    # Deuxième tâche: récupérer la valeur de XCom
    @task()
    def pull_value(ti=None):
        # Récupérer la valeur poussée par push_value
        value_from_xcom = ti.xcom_pull(task_ids='push_value')
        print(f"Valeur récupérée de XCom: {value_from_xcom}")

    # Définir l'ordre des tâches
    push = push_value()
    pull = pull_value()

    push >> pull
