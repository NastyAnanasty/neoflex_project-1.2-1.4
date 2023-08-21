import PROJ1_3

input_data = input("Введите значение вида 'YYYY-MM-DD': ")
query = f"SELECT * FROM min_max_info(TO_DATE('{input_data}', 'YYYY-MM-DD'))"
PROJ1_3.from_db_to_csv(query, 'project_1_4.csv')