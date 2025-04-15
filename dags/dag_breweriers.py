from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from extract_task import extract_all_breweries
from silver_task import transform_to_silver
from gold_task import transform_to_gold
import os

# Configurações padrão da DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
    'start_date': datetime(2025, 4, 13),
    'depends_on_past': False,
}

def log_pipeline_status(ti, **kwargs):
    execution_time = ti.xcom_pull(task_ids='extract_task', key='execution_time')
    base_path = "/opt/airflow/data/logs"
    log_dir = os.path.join(base_path, execution_time)
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "dag_status.log")

    dag_run = kwargs['dag_run']
    task_ids = ['extract_task', 'silver_task', 'gold_task']
    status_report = []

    for task_id in task_ids:
        task_instance = dag_run.get_task_instance(task_id)
        state = task_instance.state if task_instance else "unknown"
        status_report.append(f"{task_id}: {state}")

    overall_status = "success" if all("success" in s for s in status_report) else "failed"

    with open(log_file, "w", encoding="utf-8") as f:
        f.write(f"[{execution_time}] Status geral da DAG: {overall_status}\n")
        for line in status_report:
            f.write(f" - {line}\n")

    print(f"[{execution_time}] Log de execução da DAG salvo em: {log_file}")

with DAG(
    dag_id='brewery_pipeline',
    default_args=default_args,
    schedule_interval=None,
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

    log_status = PythonOperator(
        task_id='log_pipeline_status',
        python_callable=log_pipeline_status,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE
    )

    extract >> silver >> gold >> log_status

globals()["dag"] = dag
