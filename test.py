import os
from dotenv import load_dotenv
from prefect import flow, task
import mysql.connector
import datetime



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

my_cursor.execute("select created_date from g_tweeter_data_meli")

#my_cursor.execute("create temporary table duplicates_tweets as select tweeter_id from g_tweeter_data_meli group by tweeter_id having count(*)>1")

#my_cursor.execute("delete from g_tweeter_data_meli where tweeter_id in (select tweeter_id from duplicates_tweets)")
                  

#mydb.commit()
results = my_cursor.fetchall()
print(results)

my_cursor.close()
mydb.close()
