import psycopg2
import csv


def from_db_to_csv(query, csv_filename):
    conn = psycopg2.connect(user='postgres',
                            password='',
                            host='localhost',
                            port='5432',
                            database='check')

    cursor = conn.cursor()

    insert_log_query = "INSERT INTO import_export_logs (message) VALUES (%s)"
    cursor.execute(insert_log_query, ("Starting export operation",))
    conn.commit()

    cursor.execute(query)

    results = cursor.fetchall()

    with open(csv_filename, 'w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)

        column_names = [desc[0] for desc in cursor.description]
        csv_writer.writerow(column_names)

        for row in results:
            csv_writer.writerow(row)

    insert_log_query = "INSERT INTO import_export_logs (message) VALUES (%s)"
    cursor.execute(insert_log_query, ("Export operation completed",))
    conn.commit()
    cursor.close()
    conn.close()


def from_csv_to_db(csv_filename):
    conn = psycopg2.connect(user='postgres',
                            password='',
                            host='localhost',
                            port='5432',
                            database='check')

    cursor = conn.cursor()

    get_table_structure_query = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'dm_f101_round_f' ORDER BY ordinal_position"
    cursor.execute(get_table_structure_query)
    table_structure = cursor.fetchall()

    create_table_query = f"CREATE TABLE IF NOT EXISTS {'DMA.dm_f101_round_f_v2'} ({', '.join([f'{col[0]} {col[1]}' for col in table_structure])})"
    cursor.execute(create_table_query)

    insert_log_query = "INSERT INTO import_export_logs (message) VALUES (%s)"
    cursor.execute(insert_log_query, ("Starting import operation",))
    conn.commit()

    with open(csv_filename, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)

        column_names = next(csv_reader)

        for row in csv_reader:
            row = [col if col != '' else None for col in row]
            insert_query = f"INSERT INTO DMA.dm_f101_round_f_v2 ({', '.join(column_names)}) VALUES ({', '.join(['%s'] * len(column_names))})"
            cursor.execute(insert_query, row)

    insert_log_query = "INSERT INTO import_export_logs (message) VALUES (%s)"
    cursor.execute(insert_log_query, ("Import operation completed",))
    conn.commit()

    conn.commit()
    cursor.close()
    conn.close()


from_db_to_csv("SELECT * FROM DMA.dm_f101_round_f", "dm_f101_round_f.csv")
from_csv_to_db("dm_f101_round_f.csv")
