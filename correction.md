# 📄 Corrigé Détaillé - TD Airflow : Pipeline Ecommerce

## 🏢 **Introduction**
Ce corrigé détaille les solutions attendues pour chaque étape du TD. L'objectif était de construire un pipeline Airflow avancé en intégrant plusieurs concepts d'orchestration et de gestion des workflows.

---

## **1️⃣ Création du DAG de base**

### **Fichier :** `dags/ecommerce_order_processing.py`
#### 🔹 Code attendu :
```python
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime

# Configuration des variables Airflow
default_warehouse = Variable.get("default_warehouse", default_var="WH001")
threshold_stock = int(Variable.get("threshold_stock", default_var="10"))

# Définition du DAG
with DAG(
    dag_id="ecommerce_order_processing",
    schedule_interval="@daily",
    start_date=datetime(2025, 2, 24),
    catchup=False
) as dag:
    
    @task
    def check_variables():
        print(f"Warehouse: {default_warehouse}, Stock Threshold: {threshold_stock}")

    check_variables()
```

### **📊 Validation attendue**
- Un DAG avec un `schedule_interval="@daily"` et `catchup=False`.
- La tâche `check_variables()` affiche correctement les variables Airflow.

---

## **2️⃣ Ajout du branching avec `BranchPythonOperator`**

### **Mise à jour du DAG**
```python
from airflow.operators.python import BranchPythonOperator
import random

@task.branch
def check_stock():
    current_stock = random.randint(0, 20)
    print(f"Current stock in {default_warehouse}: {current_stock}")
    
    if current_stock >= threshold_stock:
        return "process_order"
    else:
        return "reject_order"

@task
def process_order():
    print("✅ The order is validated and being processed.")

@task
def reject_order():
    print("❌ The order is rejected, insufficient stock.")

branch_decision = check_stock()
branch_decision >> [process_order(), reject_order()]
```

### **📊 Validation attendue**
- `check_stock()` **choisit dynamiquement** entre `process_order` et `reject_order`.
- `BranchPythonOperator` fonctionne correctement.

---

## **3️⃣ Utilisation de `TaskGroup`**

### **Ajout de `TaskGroup`**
```python
from airflow.decorators import task_group

@task_group(group_id="order_processing")
def order_processing_group():
    @task
    def process_order():
        print("✅ The order is validated and being processed.")

    @task
    def reject_order():
        print("❌ The order is rejected, insufficient stock.")
    
    return process_order(), reject_order()

process_order_task, reject_order_task = order_processing_group()
```

### **📊 Validation attendue**
- `TaskGroup` regroupe `process_order` et `reject_order` pour une meilleure lisibilité.

---

## **4️⃣ Intégration de `Jinja Templating` et `XCom`**

### **Fichier SQL** (`sql/check_stock.sql`)
```sql
SELECT stock_quantity FROM stock
WHERE warehouse_id = '{{ var.value.default_warehouse }}'
AND processing_date = '{{ ds }}';
```

### **Ajout du `SqliteOperator` et `XCom`**
```python
from airflow.operators.sqlite_operator import SqliteOperator

check_stock_sqlite = SqliteOperator(
    task_id="check_stock_sqlite",
    sqlite_conn_id="sqlite_default",
    sql="sql/check_stock.sql",
    do_xcom_push=True
)
```

### **📊 Validation attendue**
- `Jinja` est utilisé pour injecter dynamiquement `default_warehouse` et `ds`.
- `XCom` permet de transmettre les résultats SQL.

---

## **5️⃣ Scheduling avancé et orchestration**

### **Ajout de `FileSensor`**
```python
from airflow.sensors.filesystem import FileSensor

wait_for_orders = FileSensor(
    task_id="wait_for_orders",
    filepath="/path/to/orders.csv",
    fs_conn_id="fs_default",
    poke_interval=60,
    timeout=600,
    mode="reschedule"
)
```

### **Ajout de `TriggerDagRunOperator`**
```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

trigger_email_dag = TriggerDagRunOperator(
    task_id="trigger_email_dag",
    trigger_dag_id="send_order_confirmation",
    wait_for_completion=False
)
```

### **📊 Validation attendue**
- `FileSensor` attend correctement un fichier avant d'exécuter le DAG.
- `TriggerDagRunOperator` lance un autre DAG.

---

## **6️⃣ Bonnes pratiques et tests**

### **Ajout du `logging`**
```python
import logging

logging.info("Processing stock check")
```

### **Ajout de tests unitaires (`tests/test_dag.py`)**
```python
import pytest
from dags.ecommerce_order_processing_advanced import check_stock

def test_check_stock_high_stock(mocker):
    mocker.patch("airflow.models.Variable.get", return_value="10")
    assert check_stock([[15]]) == "order_processing.process_order"
```

### **📊 Validation attendue**
- `logging` est utilisé au lieu de `print()`.
- Les tests unitaires passent avec `pytest`.

---

## **🌟 Conclusion**
Ce corrigé a permis de valider toutes les étapes du TD en fournissant des solutions claires et documentées.

Le pipeline Airflow est maintenant **optimisé, robuste et maintenable** ! 🚀

