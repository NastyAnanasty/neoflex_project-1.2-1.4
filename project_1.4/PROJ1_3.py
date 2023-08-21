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
