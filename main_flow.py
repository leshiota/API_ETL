from prefect import flow
from tweets_data import twitter_api
from transformation import transformation_tweets

flow(name='flow_orchestrator')
def main_flow():
    twitter_api()
    transformation_tweets()

if __name__ == "__main__":
    main_flow()