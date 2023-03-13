# create an airflow dag with two tasks, one that create a table in a postgres database and another that insert data on the same database

from airflow.decorators import dag, task

from datetime import datetime, timedelta
from airflow import AirflowException
from airflow.utils.dates import days_ago
from airflow.models import Variable
import requests
import base64




# default arguments
default_args = {
    "owner": "israel siqueira",
    "start_date": days_ago(2),    
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

@dag(
    default_args=default_args,
    schedule_interval=timedelta(minutes=30),
    description="Get Spotify API token and refresh it",
    catchup=False,
)
def refresh_spotify_api_token_dag():
    @task()
    def get_token():
        clientId = Variable.get("SPOTIFY_CLIENT_ID")
        clientSecret = Variable.get("SPOTIFY_CLIENT_SECRET")

        encoded = base64.b64encode(
            (clientId + ":" + clientSecret).encode("ascii")
        ).decode("ascii")

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "Basic " + encoded,
        }

        payload = {"grant_type": "client_credentials"}

        req = requests.Session()
        response = req.post(
            "https://accounts.spotify.com/api/token",
            data=payload,
            headers=headers,
            timeout=5,
        )
        return response.json()

    @task()
    def update_token(payload):
        Variable.update("SPOTIFY_API_TOKEN", payload["access_token"])

    payload = get_token()
    update_token(payload)


refresh_spotify_api_token_dag = refresh_spotify_api_token_dag()
