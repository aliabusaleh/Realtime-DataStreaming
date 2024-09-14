from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests

from kafka import KafkaProducer
import time
import logging

default_args = {
    'owner': 'Ali',
    'start_date': datetime(2024, 9, 14, 22, 00)
}

def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    return res


def format_data(response):
    data = {}
    data['first_name'] = response['name']['first']
    data['last_name'] = response['name']['last']
    data['gender'] = response['gender']
    location = response['location']

    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}"\
                        f"{location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['registered_date'] = response['registered']['date']
    data['phone'] = response['phone']
    data['picture'] = response['picture']['medium']
    return data



def stream_data():
    

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()
    while True:
        if time.time() > curr_time + 60: #1 mins
            break
        try:
            res = get_data()
            f_res = format_data(response=res)
            producer.send('Users_created', json.dumps(f_res).encode('utf-8'))
        except Exception as e:
            logging.error(f"An error occured: {e}")
            continue
    


with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    streamin_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

