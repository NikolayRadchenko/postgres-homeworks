"""Скрипт для заполнения данными таблиц в БД Postgres."""
import os

import psycopg2
from psycopg2 import OperationalError


PATH: str = os.getenv('Path_to_postgres-homework-1')


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port)
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f'The error "{e}" occurred')
    return connection


def execute_data(table, columns, from_, conn):
    with conn.cursor() as cursor:
        cursor.execute(
            f"COPY {table}({columns})"
            f"FROM '{from_}'"
            "DELIMITER ','"
            "CSV HEADER;")


connection = create_connection('north', 'postgres', '335734', 'localhost', '5432')
execute_data('employees', '"employee_id","first_name","last_name","title","birth_date","notes"',
             f"{PATH}/employees_data.csv", conn=connection)
execute_data('customers', '"customer_id","company_name","contact_name"',
             f"{PATH}/customers_data.csv", conn=connection)
execute_data('orders', '"order_id","customer_id","employee_id","order_date","ship_city"',
             f"{PATH}/orders_data.csv", conn=connection)

connection.commit()
connection.close()
