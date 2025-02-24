from airflow import DAG
from airflow.decorators import task, task_group
from datetime import datetime

# Initialisation du DAG
default_args = {
    'start_date': datetime(2024, 10, 1),
    "owner": "wilfried", 
    "retries": 3 
}

# Exemple de données clients
client_data = [
    {"id": 1, "name": "Alice", "age": 25},
    {"id": 2, "name": "Bob", "age": None},
    {"id": 3, "name": "Charlie", "age": 30},
    {"id": 4, "name": "David", "age": 25},
    {"id": 4, "name": "David", "age": 25},  # Doublon
]

# Tâches de nettoyage avec le décorateur @task_group
@task_group(group_id='data_cleaning')
def cleaning_tasks(data):
    
    @task()
    def remove_duplicates(data):
        unique_data = [dict(t) for t in {tuple(d.items()) for d in data}]
        print(f"Data après suppression des doublons: {unique_data}")
        return unique_data

    @task()
    def handle_missing_values(data):
        for record in data:
            if record['age'] is None:
                record['age'] = 0  # Remplacer les valeurs manquantes par 0
        print(f"Data après gestion des valeurs manquantes: {data}")
        return data

    clean_data = remove_duplicates(data)
    final_clean_data = handle_missing_values(clean_data)
    return final_clean_data

# Tâches d'analyse des données avec @task_group
@task_group(group_id='data_analysis')
def analysis_tasks(cleaned_data):
    
    @task()
    def calculate_average_age(data):
        total_age = sum([d['age'] for d in data])
        count = len(data)
        avg_age = total_age / count if count > 0 else 0
        print(f"Âge moyen des clients: {avg_age}")
        return avg_age

    @task()
    def generate_summary(data):
        report = {
            "total_clients": len(data),
            "average_age": sum(d['age'] for d in data) / len(data),
        }
        print(f"Résumé du rapport: {report}")
        return report

    average_age = calculate_average_age(cleaned_data)
    summary = generate_summary(cleaned_data)

# Définition du DAG
with DAG('exercice_task_group_real_data', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    @task()
    def download_data():
        print("Téléchargement des données clients...")
        return client_data

    start = download_data()

    # Appel des groupes de tâches
    cleaned_data = cleaning_tasks(start)
    analysis = analysis_tasks(cleaned_data)
