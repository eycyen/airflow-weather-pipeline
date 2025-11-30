from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2
import json


def etl_process():

    print("Veri çekiliyor...")
    url = "http://api.weatherapi.com/v1/current.json?key={APIKEY}&q=Ankara&aqi=no"
    r = requests.get(url)
    mydata = r.json()
    
    location = mydata["location"]
    current = mydata["current"]
    

    current_f = current["temp_f"]
    current_c = (current_f - 32.0) * (5.0/9.0)
    current_c = float("{:.2f}".format(current_c))
    
    local_date_obj = datetime.strptime(location["localtime"], "%Y-%m-%d %H:%M")
    local_date = local_date_obj.strftime("%Y-%m-%d")
    local_time = local_date_obj.strftime("%H:%M:%S")
    
    try:
        con = psycopg2.connect(
            host="host.docker.internal", 
            database="postgres",
            user="postgres",
            password="{password}",
            port="5432"
        )
        cursor = con.cursor()
        
        insert_query = """
        INSERT INTO weather (City, Country, Date, Time, Temperature_C, Weather) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        data_tuple = (
            location["name"], 
            location["country"], 
            local_date, 
            local_time, 
            current_c, 
            current["condition"]["text"]
        )
        
        cursor.execute(insert_query, data_tuple)
        con.commit()
        cursor.close()
        con.close()
        print("Veri başarıyla PostgreSQL'e yüklendi!")
        
    except Exception as e:
        print(f"HATA OLUŞTU: {e}")
        raise e

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hava_durumu_takip',
    default_args=default_args,
    description='Ankara hava durumunu kaydeden ETL süreci',
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    run_etl = PythonOperator(
        task_id='ankara_hava_durumu_cek',
        python_callable=etl_process,
    )


    run_etl
