import psycopg2
import psycopg2.extras
import pandas as pd


def load(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='transform')

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






