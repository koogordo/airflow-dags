from airflow import DAG
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
import logging
import airflow.utils
from datetime import timedelta

# create a logger
logger = logging.getLogger('airflow.task')

# Replace the following with values appropriate for your use
conn_id = 'snowflake_dev'


def get_conn():
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    return hook.get_conn()


def get_table_names(**kwargs):
    task_instance = kwargs['ti']
    with get_conn() as conn:
        c = conn.cursor()
        try:
            table_names = c.execute('SELECT TABLE_NAME FROM SNOWFLAKE_SAMPLE_DATA.INFORMATION_SCHEMA.TABLES').fetchall()
            task_instance.xcom_push(key='table_names', value=table_names)
        finally:
            c.close()


def log_col_name(**kwargs):
    task_instance = kwargs['ti']
    with get_conn() as conn:
        c = conn.cursor()
        try:

            col_name = c.execute(
                'SELECT COLUMN_NAME FROM SNOWFLAKE_SAMPLE_DATA.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s LIMIT 1',
                task_instance.xcom_pull(task_ids='get_table_names', key='table_names')[0][0]
            ).fetchone()
            logger.info(f'Task executed successfully with result {col_name[0]}')
        finally:
            c.close()


# create a dictionary type variable that will hold the default args for the DAG
args = {
    "owner": "ocho",
    "start_date": airflow.utils.dates.days_ago(1),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}

# create DAG
dag = DAG(
    dag_id='test_xcom',
    default_args=args
)

with dag:
    table_names = PythonOperator(
        task_id='get_table_names',
        python_callable=get_table_names,
        provide_context=True
    )

    log_col_name = PythonOperator(
        task_id='log_col_name',
        python_callable=log_col_name,
        provide_context=True
    )

    table_names >> log_col_name
