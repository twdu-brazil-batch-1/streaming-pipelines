import json
import subprocess
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.hooks import SSHHook

from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': None,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

sshHook = SSHHook(conn_id="emr-master.twdu-brazil-batch-1.training")

dag = DAG('monitoring_CSV_1', default_args=default_args)

read_csv_cmd = """
export AWS_DEFAULT_REGION=us-east-2

echo =======GET CREATION TIME========

csv_create_time='hadoop fs -stat "%y" /tw/stationMart/data/_SUCCESS'

echo csv_create_time

echo =====COMPARE DATES========
"""

read_csv_task = SSHExecuteOperator(
    task_id="read_csv_file",
    bash_command=read_csv_cmd,
    ssh_hook=sshHook,
    dag=dag)

read_csv_task