import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook


def initialise_db_cursor(schema, mysql_conn_id):
    mysql_hook = MySqlHook(schema=schema, mysql_conn_id=mysql_conn_id)
    mysql_connection = mysql_hook.get_conn()
    return mysql_connection.cursor()


def get_row_number(table_name, db_cursor):
    db_cursor.execute(f'select count(1) from {table_name}')
    return db_cursor.fetchone()[0]


def fetch_dataset(query, db_cursor):
    db_cursor.execute(query)

    columns_name = [item[0] for item in db_cursor.description]
    data = db_cursor.fetchall()

    return pd.DataFrame(data, columns=columns_name)

