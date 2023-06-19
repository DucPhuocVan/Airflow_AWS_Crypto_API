import json
import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import requests
from airflow.hooks.S3_hook import S3Hook

def get(url:str):
    now=datetime.now()
    now=f"{now.year}-{now.month}-{now.day}T{now.hour}-{now.minute}-{now.second}"
    res=requests.get(url)
    res=json.loads(res.text)
    dt=res
    if not dt:
       raise Exception('no data') 
    df=pd.DataFrame(dt)
    with open(f"/home/vdp/airflow/data/crypto.csv", 'w') as f:
        df.to_csv(f, index=False)
    with open(f"/home/vdp/airflow/data/crypto.json", 'w') as f:
        json.dump(res, f)

def upload_to_s3(filename: str, bucket_name: str) -> None:
    now=datetime.now()
    now=f"{now.year}-{now.month}-{now.day}-{now.hour}-{now.minute}-{now.second}"
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=f"data/crypto-{now}.json", bucket_name=bucket_name)

with DAG(
    dag_id='api_s3_dag',
    schedule_interval='@daily',
    start_date=datetime(2023, 6, 12),
    catchup=False
) as dag:
    # get data & save data to local
    task_get_posts = PythonOperator(
        task_id='get_posts',
        python_callable=get,
        op_kwargs={'url':'https://coinmap.org/api/v1/venues/'}
    )

    # Upload the file
    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': '/home/vdp/airflow/data/crypto.json',
            'bucket_name': 'alivebook-airflow-aws'
        }
    )

    #pineline
    task_get_posts >> task_upload_to_s3