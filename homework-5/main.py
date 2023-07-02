import json

import psycopg2

from config import config


def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'
    db_name = 'commodity_company'

    params = config()
    conn = None

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                add_supplier_id(cur)
                print("Столбец supplier_id в таблицу products успешно добавлен")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_suppliers_id(cur, suppliers)
                print("Suppliers_id успешно добавлены")

                add_foreign_keys(cur)
                print(f"FOREIGN KEY успешно добавлены")

                conn.commit()
                conn.close()

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    connection = psycopg2.connect(dbname='postgres', **params)
    connection.autocommit = True
    cur = connection.cursor()

    cur.execute(f'DROP DATABASE {db_name}')
    cur.execute(f'CREATE DATABASE {db_name}')

    connection.commit()
    cur.close()
    connection.close()


def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    with open(script_file, encoding='UTF-8') as file:
        sql_script = file.read()
        cur.execute(sql_script)


def create_suppliers_table(cur) -> None:
    """Создает таблицу suppliers."""
    cur.execute("""
                CREATE TABLE suppliers(
                    supplier_id serial PRIMARY KEY,
                    company_name VARCHAR NOT NULL,
                    contact VARCHAR,
                    address VARCHAR,
                    phone VARCHAR,
                    fax VARCHAR,
                    homepage VARCHAR
                    )
                """)


def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    data = []
    with open(json_file, encoding='UTF-8') as file:
        data_json = json.load(file)
        for items in data_json:
            data_company = []
            for key, value in items.items():
                if value != "":
                    data_company.append(value)
                else:
                    data_company.append(None)
            data.append({items['company_name']: data_company})
    return data


def insert_suppliers_data(cur, suppliers: list[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    for items in suppliers:
        for key, value in items.items():
            cur.execute(f"""
                        INSERT INTO suppliers(company_name, contact, address, phone, fax, homepage) 
                        VALUES ('{value[0]}', '{value[1]}', '{value[2]}', '{value[3]}', '{value[4]}', '{value[5]}')
            """)


def add_supplier_id(cur) -> None:
    cur.execute(f"""
                    ALTER TABLE products ADD COLUMN supplier_id int;
        """)


def add_suppliers_id(cur, data) -> None:
    i = 1
    for items in data:
        for key, value in items.items():
            for k in range(len(value[6])):
                cur.execute(f"""
                            UPDATE products SET supplier_id = {i} WHERE product_name = '{value[6][k]}' 
                """)
        i += 1


def add_foreign_keys(cur) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""
    cur.execute(f"""
                    ALTER TABLE ONLY products
                    ADD CONSTRAINT fk_products_suppliers FOREIGN KEY(supplier_id) REFERENCES suppliers;
        """)


if __name__ == '__main__':
    main()
