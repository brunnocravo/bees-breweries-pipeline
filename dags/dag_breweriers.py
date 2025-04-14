from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from extract_task import extract_all_breweries
from silver_task import transform_to_silver
from gold_task import transform_to_gold

# Configurações padrão da DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2025, 4, 13),
    'depends_on_past': False,
}

with DAG(
    dag_id='brewery_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Execução manual
    catchup=False,
    tags=['brewery', 'etl', 'case'],
    description='Pipeline completo: API → Bronze → Silver → Gold'
) as dag:

    def extract_task(ti):
        execution_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        ti.xcom_push(key='execution_time', value=execution_time)
        extract_all_breweries(execution_time=execution_time)

    def silver_task(ti):
        execution_time = ti.xcom_pull(task_ids='extract_task', key='execution_time')
        transform_to_silver(execution_time=execution_time)

    def gold_task(ti):
        execution_time = ti.xcom_pull(task_ids='extract_task', key='execution_time')
        transform_to_gold(execution_time=execution_time)

    extract = PythonOperator(
        task_id='extract_task',
        python_callable=extract_task
    )

    silver = PythonOperator(
        task_id='silver_task',
        python_callable=silver_task
    )

    gold = PythonOperator(
        task_id='gold_task',
        python_callable=gold_task
    )

    extract >> silver >> gold
