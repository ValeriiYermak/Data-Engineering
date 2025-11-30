from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'batch_data_lake_pipeline',
    default_args=default_args,
    description='Batch Data Lake Pipeline - Bronze to Silver to Gold',
    schedule_interval=None,
    catchup=False,
    tags=['data_engineering', 'spark', 'datalake']
)

start_task = DummyOperator(task_id='start_pipeline', dag=dag)

# Використовуємо BashOperator замість SparkSubmitOperator
landing_to_bronze_task = BashOperator(
    task_id='landing_to_bronze',
    bash_command='cd /opt/airflow/dags && python landing_to_bronze.py',
    dag=dag,
)

bronze_to_silver_task = BashOperator(
    task_id='bronze_to_silver',
    bash_command='cd /opt/airflow/dags && python bronze_to_silver.py',
    dag=dag,
)

silver_to_gold_task = BashOperator(
    task_id='silver_to_gold',
    bash_command='cd /opt/airflow/dags && python silver_to_gold.py',
    dag=dag,
)

final_check_task = BashOperator(
    task_id='final_check',
    bash_command='cd /opt/airflow/dags && python final_check.py',
    dag=dag,
)

end_task = DummyOperator(task_id='end_pipeline', dag=dag)

start_task >> landing_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task >> final_check_task >> end_task
