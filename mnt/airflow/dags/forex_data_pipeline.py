import airflow
from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
#from airflow.operators.slack_operator import SlackAPIPostOperator

import requests
import json
from datetime import datetime, timedelta
import os
from collections import defaultdict

import pandas as pd

default_args = {
            "owner": "Airflow",
            "start_date": airflow.utils.dates.days_ago(1),
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "email": "pedrocj@gmail.com",
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        }

#Download forex rates according to the currencies we want to watch
#  described in the file forex_currencies.csv
def download_rates():
    url = 'https://api.exchangeratesapi.io/latest'

    response = requests.get(url)

    data = json.loads(response.text)

    df_data = defaultdict(list)

    date = data.get('date')
    base = data.get('base')

    for rate, value in data.get('rates').items():
        print(date, base, rate, value)
        df_data['date'].append(date)    
        df_data['base'].append(base)    
        df_data['rate'].append(rate)    
        df_data['value'].append(value)

    df = pd.DataFrame(df_data)

    print(df.head())

    save_folder = '/usr/local/airflow/dags/files'

    output_file = os.path.join(save_folder,f'{date}_fx_rates_base_{base}.csv')

    df.to_csv(output_file, index=False)

   

with DAG(dag_id="forex_data_pipeline", schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

    # Checking if forex rates are avaiable
    is_forex_rates_available = HttpSensor(
            task_id="is_forex_rates_available",
            method="GET",
            http_conn_id="forex_api",
            endpoint="latest",
            response_check=lambda response: "rates" in response.text,
            poke_interval=5,
            timeout=20
    )

    # downloading the rates using requests and saving in csv file with date of download

    downloading_rates = PythonOperator(
            task_id="downloading_rates",
            python_callable=download_rates
    )

    # Sending a notification by email
    # https://stackoverflow.com/questions/51829200/how-to-set-up-airflow-send-email


    # sending_email_notification = EmailOperator(
    #         task_id="sending_email",
    #         to="airflow_course@yopmail.com",
    #         subject="forex_data_pipeline",
    #         html_content="""
    #             <h3>forex_data_pipeline succeeded</h3>
    #         """
    #         )

    is_forex_rates_available >> downloading_rates 