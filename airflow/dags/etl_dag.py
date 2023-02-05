from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
# from . import extract,transform,load
import pandas as pd
import psycopg2
import psycopg2.extras
import numpy as np

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_pipeline_3',
    default_args=default_args,
    description='A simple ETL pipeline in Airflow',
    schedule_interval=timedelta(hours=1),
)

def extract():
    """
       function for extracting 
          data from a csv file
    """
    avito_df=pd.read_csv('avito.csv')
    
    return avito_df


def fill_missing_values(data):
    #replacing Nan(string) with np.nan 
    data=data.replace({'Nan':np.nan})

    ### filling missing values with mode for categorical variables and with mean for continuous variables
    data=data.fillna({"Etat":data["Etat"].mode()[0],
                                "price":data['price'].mean(),
                                        "Number_door":data['Number_door'].mode()[0],
                                                "first_hand":data['first_hand'].mode()[0],
                                                            "origin":data['first_hand'].mode()[0] })

    return data                                                




def drop_columns(data):
    ##" removing unimportant columns
    data=data.drop(["name","Secteur","type"],axis=1)
    return data


def kilometrage(data):

    data["minimum_distance"]=data["kilometrage"].str.split("-").apply(lambda x:x[0].replace(" ",""))
    data["maximum_distance"]=data["kilometrage"].str.split("-").apply(lambda x:x[1].replace(" ",""))
    data["minimum_distance"]=data["minimum_distance"].astype(int)
    data["maximum_distance"]=data["maximum_distance"].astype(int)
   


    return data

def transform(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='extract')
    data=fill_missing_values(data)
    data=drop_columns(data)
    data=kilometrage(data)

    return data



def load(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='transform')

    ### deployement in postgres container
    hostname = "localhost"
    database = "exampledb"
    username = "docker"
    pwd = "docker"
    port_id = 5431
    conn = None

    try:
        with psycopg2.connect(
                    host = hostname,
                    dbname = database,
                    user = username,
                    password = pwd,
                    port = port_id) as conn:

            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

                cur.execute("DROP TABLE IF EXISTS cars_feature")

                create_script = "CREATE TABLE if not exists \
                                cars_feature (cars_id SERIAL,name text, price float,type text,\
                                        number_door text,kilometrage text,first_hand text,\
                                            secteur text,marque text,etat text,year_model integer,origin text)"
                cur.execute(create_script)

                count=0
                for car in df.iterrows():
                    tuples=tuple(car[1])


                    cur.execute(
                            f""" INSERT INTO cars_feature(name, price, type, number_door, kilometrage, first_hand, secteur, marque, etat, year_model, origin) 
                                VALUES('{tuples[0]}','{tuples[1]}','{tuples[2]}','{tuples[3]}','{tuples[4]}','{tuples[5]}','{tuples[6]}','{tuples[7]}','{tuples[8]}','{tuples[9]}','{tuples[10]}')""")


                count +=1
    except Exception as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()












### defining pipeline tasks

extract_task=PythonOperator(
        task_id='extract',
        python_callable=extract,
        dag=dag,
)

transform_task=PythonOperator(
        task_id='transform',
        python_callable=transform,
        dag=dag,
        provide_context=True,
)

load_task=PythonOperator(
        task_id='load',
        python_callable=load,
        dag=dag,
        provide_context=True,
)

#defining pipeline

extract_task >> transform_task >> load_task



















