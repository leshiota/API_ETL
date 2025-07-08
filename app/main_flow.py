
from prefect import flow
from flow.tweets_data import twitter_api
from flow.transformation import transformation_tweets
from flow.database import database_create



flow
def main_flow():
    database_create()
    twitter_api()
    transformation_tweets()

if __name__ == "__main__":
    main_flow()