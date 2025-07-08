import os
from dotenv import load_dotenv
from prefect import flow, task
import mysql.connector



load_dotenv()


def get_connection_databse():
    """Connect with database"""
    return mysql.connector.connect(
        host = "db",
        user = os.getenv("user_db"),
        passwd =os.getenv("passwd_db"),
        database = "tweets_data",
        auth_plugin = "mysql_native_password"
    )

def get_connection():
    """Connect with database"""
    return mysql.connector.connect(
        host = "db",
        user = os.getenv("user_db"),
        passwd =os.getenv("passwd_db"),
        auth_plugin = "mysql_native_password"
    )


@task
def create_database(database_name: str):
    """Create database"""
    mydb = get_connection()

    my_cursor = mydb.cursor()

    my_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    my_cursor.close()
    mydb.close()



@flow()
def database_create():
    database = create_database('tweets_data')


