from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 9),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': None,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def read_csv_timestamp():
    return """
      export TIMESTAMP=$(ssh -tt emr-master.twdu-brazil-batch-1.training bash -c "'hadoop fs -stat "%y" /tw/stationMart/data/_SUCCESS'")
      
      echo $TIMESTAMP
    """

def is_csv_updated(**context):
    last_csv_update = context['task_instance'].xcom_pull(task_ids='read_csv_file', key='return_value')
    print("CSV file was last updated at", last_csv_update)
    return last_csv_update


with DAG('CSV_monitor_1', 
    default_args=default_args,
    schedule_interval=None,
    catchup=False) as dag:

    read_csv_task = BashOperator(
        task_id="read_csv_file",
        bash_command=read_csv_timestamp(),
        xcom_push=True
    )

    is_updated = PythonOperator(
      task_id="is_updated",
      python_callable=is_csv_updated,
      provide_context=True
    )

    read_csv_task >> is_updated