import time
import mysql.connector
import os

while True:
    try:
        conn = mysql.connector.connect(
            host = "db",
            user = os.getenv("user_db"),
            passwd =os.getenv("passwd_db"),
            database = "tweets_data",
            auth_plugin = "mysql_native_password"
    )

        
        conn.close()
        break
    except Exception as e:
        print("Waiting for MySQL...", e)
        time.sleep(1)

