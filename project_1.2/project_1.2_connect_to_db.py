import psycopg2
import pandas as pd
import time


def run_101():
    try:
        connection = psycopg2.connect(user='postgres',
                                      password='',
                                      host='localhost',
                                      port='5432',
                                      database='check')
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute("CALL DMA.fill_f101_round_f(TO_DATE('2018-01-15', 'YYYY-MM-DD'));")
        cursor.execute("SELECT * FROM DMA.dm_f101_round_f")
        colnames = [desc[0] for desc in cursor.description]
        data = []
        for row in cursor.fetchall():
            data.append(row)
        df = pd.DataFrame(data, columns=colnames)
        print(df)
        cursor.close()
        connection.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to PostgreSQL", error)


for i in range(10):
    run_101()
    time.sleep(60)
