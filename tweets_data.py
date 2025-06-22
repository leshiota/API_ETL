import tweepy
import os
from dotenv import load_dotenv
import pandas as pd
from prefect import flow, task
import requests
import mysql.connector
import json


load_dotenv()
bearer_token = os.getenv("bearer_token")
user_db = os.getenv("user_db")
passwd_db = os.getenv("passwd_db")
search_query = '"Mercado livre" "Frete grátis" "R$19" -is:retweet -is:reply -has:links'
number_of_tweets = 10
pg_user = os.getenv("user_db")
pg_password = os.getenv("passwd_db")



@task(name ='token_connection')
def connection(bearer_token: str):
    """Create the connection with X API"""
    return tweepy.Client(bearer_token)

#@task(name='tweet_data')
#def tweet_data(query: str, max_results: int = 100, client):
#    """Fetch tweets using the X API."""
#    response = client.search_recent_tweets(
#        query=query,
#        tweet_fields=["created_at", "public_metrics", "author_id","text"]
#        max_results=max_results
#    )
#    tweets = response.data or []
#    return [
#        {
#            "id": tweet.id,
#            "created_at": tweet.created_at,
#            "author_id": tweet.author_id,
#            "likes" : tweet.public_metrics["like_count"],
#            "text": tweet.text
#        }
#        for tweet in tweets
#    ]


@task
def conect_database(pg_user: str,pg_password: str):
    """Create the connection with Mysql"""
    host_db = "localhost"
    database_name = "tweets_data"


    return mysql.connector.connect(
        host = "localhost",
        user = pg_user,
        passwd = pg_password,
        database = 'tweets_data',
        auth_plugin = "mysql_native_password")


@task
def create_table(pg_user, pg_password):
    """Create table"""
    mydb =mysql.connector.connect(
        host = 'localhost',
        user = pg_user,
        passwd = pg_password,
        database = 'tweets_data',
        auth_plugin = "mysql_native_password")

    my_cursor = mydb.cursor()

    return my_cursor.execute(f"CREATE TABLE IF NOT EXISTS tweet_data_meli (tweeter_id bigint, created_at timestamp, author_id VARCHAR(200),likes int,text VARCHAR(10000))")


@task
def tweets_information(client, search_query, number_of_tweets, pg_user,pg_password):
    mydb =mysql.connector.connect(
                host = 'localhost',
                user = pg_user,
                passwd = pg_password,
                database = 'tweets_data',
                auth_plugin = "mysql_native_password")
    
    my_cursor = mydb.cursor()
    try:
        response = client.search_recent_tweets(
            query=search_query,
            max_results=number_of_tweets,
            tweet_fields=["created_at", "public_metrics", "author_id","text"]
        )
        #for tweet in response.data:
            #print(json.dumps(tweet.data, indent=2, ensure_ascii=False)) 
        if response.data:
            for tweet in response.data:
                id = tweet.id
                created_at = tweet.created_at
                author_id = tweet.author_id
                likes = tweet.public_metrics["like_count"]
                text  = tweet.text

                my_cursor.execute('INSERT INTO tweet_data_meli (tweeter_id, created_at, author_id, likes, text) VALUES (%s,%s,%s,%s,%s)',(id,created_at, author_id, likes, text))
                mydb.commit()
            my_cursor.close()
            mydb.close()
        else:
            print("Nenhum tweet encontrado")

    except tweepy.TooManyRequests:
        print("Erro: Limite de requisições excedido. Tente novamente mais tarde.")
    except Exception as e:
        print("Erro ao buscar tweets:", str(e))

@task
def first_tweet_created ():
    mydb =mysql.connector.connect(
                host = 'localhost',
                user = pg_user,
                passwd = pg_password,
                database = 'tweets_data',
                auth_plugin = "mysql_native_password")
    
    my_cursor = mydb.cursor()
    my_cursor.execute('Select min(created_at) from tweet_data_meli')
    result = my_cursor.fetchall()
    my_cursor.close()
    mydb.close()
    return result

@flow(name='Twitter_ETL')
def twitter_etl():
    client = connection(bearer_token)
    table = create_table(pg_user, pg_password)
    data_api = tweets_information(client, search_query, number_of_tweets, pg_user,pg_password)
    result = first_tweet_created ()
    print(result)







if __name__ == "__main__":
    twitter_etl()
    


