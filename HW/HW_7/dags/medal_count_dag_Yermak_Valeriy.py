from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
import random
import time

# ----------------------
# Functions
# ----------------------
def pick_medal(**kwargs):
    medal = random.choice(['Bronze', 'Silver', 'Gold'])
    return medal

def branch_medal(**kwargs):
    # Функція повертає task_id для наступного виконання (наприклад, 'calc_Gold')
    medal = kwargs['ti'].xcom_pull(task_ids='pick_medal')
    return f"calc_{medal}"

def generate_delay_func():
    time.sleep(10)

# ----------------------
# Default args
# ----------------------
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

with DAG(
    dag_id='medal_count_dag_Yermak_Valeriy',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # 0.1. НАДАННЯ ПРИВІЛЕЇВ КОРИСТУВАЧУ 'airflow' (використовує 'mysql_root')
    grant_privileges = MySqlOperator(
        task_id='grant_privileges',
        mysql_conn_id='mysql_root',
        sql="GRANT ALL PRIVILEGES ON *.* TO 'airflow'@'%' WITH GRANT OPTION;"
    )

    # 0.2. Створення бази даних 'olympic_dataset' (використовує 'mysql_default')
    create_db = MySqlOperator(
        task_id='create_db',
        mysql_conn_id='mysql_default',
        sql="CREATE DATABASE IF NOT EXISTS olympic_dataset;"
    )

    # 1. Створення таблиць (medal_counts + заглушка athlete_event_results)
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='mysql_default',
        sql=[
            """
            CREATE TABLE IF NOT EXISTS olympic_dataset.medal_counts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(10),
                count INT,
                created_at DATETIME
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS olympic_dataset.athlete_event_results (
                id INT PRIMARY KEY,
                medal VARCHAR(10)
            );
            """,
            """
            INSERT INTO olympic_dataset.athlete_event_results (id, medal) VALUES
            (1, 'Gold'), (2, 'Silver'), (3, 'Bronze')
            ON DUPLICATE KEY UPDATE medal=VALUES(medal);
            """
        ]
    )

    # 2. Pick medal (зберігає результат у XCom)
    pick_medal_task = PythonOperator(
        task_id='pick_medal',
        python_callable=pick_medal,
        provide_context=True
    )

    # 3. Branch (Визначає, яке calc_* завдання виконувати)
    pick_medal_branch = BranchPythonOperator(
        task_id='pick_medal_task', # <-- ID для гілки
        python_callable=branch_medal,
        provide_context=True
    )

    # 4. Medal calculations (Використовують заглушку)
    calc_Bronze = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id='mysql_default',
        sql="""
            INSERT INTO olympic_dataset.medal_counts (medal_type, count, created_at)
            SELECT 'Bronze',
                   COUNT(*),
                   NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Bronze';
        """
    )

    calc_Silver = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id='mysql_default',
        sql="""
            INSERT INTO olympic_dataset.medal_counts (medal_type, count, created_at)
            SELECT 'Silver',
                   COUNT(*),
                   NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Silver';
        """
    )

    calc_Gold = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id='mysql_default',
        sql="""
            INSERT INTO olympic_dataset.medal_counts (medal_type, count, created_at)
            SELECT 'Gold',
                   COUNT(*),
                   NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Gold';
        """
    )

    # 5. Delay task (без змін)
    generate_delay = PythonOperator(
        task_id='generate_delay',
        python_callable=generate_delay_func,
    trigger_rule=TriggerRule.ONE_SUCCESS
    )

    # 6. Sensor (перевіряє, чи було вставлено рядок)
    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id='mysql_default',
        sql="""
            SELECT 1
            FROM olympic_dataset.medal_counts
            WHERE created_at >= NOW() - INTERVAL 30 SECOND
            ORDER BY created_at DESC
            LIMIT 1;
        """,
        poke_interval=5,
        timeout=60,
        mode='poke'
    )

    # ----------------------
    # DAG dependencies (ОСТАТОЧНО ВИПРАВЛЕНО ЗВ'ЯЗОК BRANCH-ОПЕРАТОРА)
    # ----------------------
    grant_privileges >> create_db >> create_table >> pick_medal_task >> pick_medal_branch

    # ВИПРАВЛЕНО: Використовуємо ID 'pick_medal_task' для гілок
    pick_medal_branch >> calc_Bronze >> generate_delay
    pick_medal_branch >> calc_Silver >> generate_delay
    pick_medal_branch >> calc_Gold >> generate_delay

    generate_delay >> check_for_correctness