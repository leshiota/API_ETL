import os
from dotenv import load_dotenv
from prefect import flow, task
import mysql.connector



load_dotenv()
pg_user = os.getenv("user_db")
pg_password = os.getenv("passwd_db")


@task
def create_database(pg_user: str,pg_password: str, database_name: str):
    """Create database"""
    host_db = "localhost"


    mydb = mysql.connector.connect(
        host = host_db,
        user = pg_user,
        passwd = pg_password,
        auth_plugin = "mysql_native_password")
    
    my_cursor = mydb.cursor()

    my_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")




@flow(name='database')
def database_create():
    database = create_database(pg_user, pg_password,'tweets_data')
    #table = create_table(pg_user, pg_password, 'tweets_data')


if __name__ == "__main__":
    database_create()