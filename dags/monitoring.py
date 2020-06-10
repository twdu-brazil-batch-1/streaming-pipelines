import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta, date, time

default_args = {
    'owner': 'TwoWheelers',
    'start_date': datetime(2020, 6, 10, 21, 35),
    'depends_on_past': False
}

def read_csv_timestamp():
    return """
      export TIMESTAMP=$(ssh -tt emr-master.twdu-brazil-batch-1.training bash -c "'hadoop fs -stat "%y" /tw/stationMart/data/_SUCCESS'")
      
      echo $TIMESTAMP
    """

def convert_to_date_time(input_timestamp):
    input_timestamp_split = input_timestamp.split(" ")

    date_split = input_timestamp_split[0]
    date_list = date_split.split("-")
    convert_date_to_int = list(map(int, date_list))

    hour = input_timestamp_split[1]
    hour_list = hour.split(":")
    convert_hour_to_int = list(map(int, hour_list))

    date_converted = date(convert_date_to_int[0], convert_date_to_int[1], convert_date_to_int[2])
    hour_converted = time(convert_hour_to_int[0], convert_hour_to_int[1], convert_hour_to_int[2])

    final_date_converted = datetime.combine(date_converted, hour_converted)

    return final_date_converted

def is_csv_updated(**context):
    timestamp_from_csv = str(context['task_instance'].xcom_pull(task_ids='read_csv_file', key='return_value'))

    print("CSV file was last updated at", timestamp_from_csv)

    date_from_file = convert_to_date_time(timestamp_from_csv)

    date_now = convert_to_date_time(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("Now time is", date_now.strftime("%Y-%m-%d %H:%M:%S"))

    minutes_diff = date_now - date_from_file

    push_metric(int(minutes_diff > timedelta(minutes=5)))

    if minutes_diff > timedelta(minutes=5):
            raise ValueError('CSV file has not updated in last 5 minutes!')

def push_metric(metric_value):
    print("Publishing metric to AWS Cloudwatch ", metric_value)

    cloudwatch = boto3.client('cloudwatch', region_name='us-east-2')

    cloudwatch.put_metric_data(
        MetricData=[
            {
                'MetricName': 'files failed to update',
                'Dimensions': [
                    {
                        'Name': 'Monitoring CSV File',
                        'Value': 'read_csv_hdfs'
                    },
                ],
                'Unit': 'None',
                'Value': metric_value,
            },
        ],
        Namespace='TwoWheelers'
    )

    print("Metric published to AWS Cloudwatch ")

with DAG('CSV_monitor_1', 
    default_args=default_args,
    schedule_interval='*/5 * * * *'
    ) as dag:

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