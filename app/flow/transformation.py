import os
from dotenv import load_dotenv
from prefect import flow, task
import mysql.connector
from .database import get_connection_databse


@task
def create_table():
    mydb = get_connection_databse()
    my_cursor = mydb.cursor()
    query = "CREATE TABLE IF NOT EXISTS g_tweeter_data_meli(tweeter_id bigint, created_date timestamp, author_id VARCHAR(200),likes int,text VARCHAR(10000), content VARCHAR(200))"
    my_cursor.execute(query)
    my_cursor.close()
    mydb.close()


@task
def tranformation():  
    mydb = get_connection_databse()  
    my_cursor = mydb.cursor()
    query = "INSERT INTO g_tweeter_data_meli \
            WITH TEMP AS (SELECT tweeter_id, \
                DATE_FORMAT(created_at, '%Y-%m-%d %T') as created_date , \
               author_id, \
               likes, \
               text, \
               ROW_NUMBER() OVER(PARTITION BY author_id, text) as RN \
           FROM tweet_data_meli) \
           SELECT tweeter_id,\
                created_date,\
                author_id,\
                likes,\
                text,\
                CASE WHEN RN >1 THEN 'Tweeter content appear more than one time'  \
                    ELSE 'First time of tweeter content' \
                    END as content \
            FROM TEMP"
    my_cursor.execute(query)
    mydb.commit()
    print('Dados inseridos')
    #result = my_cursor.fetchall()

    my_cursor.close()
    mydb.close()
    #return result


@flow()
def transformation_tweets():
    create_table()
    tranformation()
    print('Running flow transformation_tweets')
    



  
