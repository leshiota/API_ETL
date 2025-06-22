import os
from dotenv import load_dotenv
from prefect import flow, task
import mysql.connector



load_dotenv()
pg_user = os.getenv("user_db")
pg_password = os.getenv("passwd_db")


mydb =mysql.connector.connect(
        host = 'localhost',
        user = pg_user,
        passwd = pg_password,
        database = 'tweets_data',
        auth_plugin = "mysql_native_password")

my_cursor = mydb.cursor()


query = my_cursor.execute("select min(created_at) from tweet_data_meli")
result = my_cursor.fetchall()
print(result)

my_cursor.close()
mydb.close()
