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
def convert_to_unix(input_timestamp):
    input_timestamp_split = input_timestamp.split(" ")

    date_split = input_timestamp_split[0]
    date_list = date.split("-")
    convert_date_to_int = list(map(int, date_list))

    hour = input_timestamp_split[1]
    hour_list = hour.split(":")
    convert_hour_to_int = list(map(int, hour_list))

    date_converted = date(convert_date_to_int[0], convert_date_to_int[1], convert_date_to_int[2])
    hour_converted = datetime.time(convert_hour_to_int[0], convert_hour_to_int[1], convert_hour_to_int[2])

    final_date_converted = datetime.datetime.combine(date_converted, hour_converted)

    return final_date_converted.strftime('%s')

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