# create an airflow dag with two tasks, one that create a table in a postgres database and another that insert data on the same database

from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator

from io import StringIO
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow.providers.google.cloud.hooks.gcs import GCSHook

from airflow.utils.dates import days_ago
from airflow import AirflowException
from airflow.models import Variable
import pandas as pd
import requests

req = requests.Session()

TOKEN = Variable.get("SPOTIFY_API_TOKEN")

BASE_URL = "https://api.spotify.com/v1"
LIMIT = 50
MARKET = "BR"  # para limitar apenas show exibidos no Brasil
TYPE = "show"  # limita a busca apenas a shows (podcasts)
QUERY_PARAM = "data%20hackers"


TB_NAME = "raw_data_hackers_podcasts"

headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Bearer {TOKEN}",
}

# default arguments
default_args = {
    "owner": "israel siqueira",
    "start_date": days_ago(2),    
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}

@dag(
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    description="Collect data hackers podcasts from spotify api and store it on a postgres database",
)
def data_hackers_podcasts_dag():
    DATASET_NAME = "data_hackers"
    TB_NAME = "raw_podcasts"

    schema_fields = [
            {"name": "id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "description", "type": "STRING", "mode": "REQUIRED"},
            {"name": "total_episodes", "type": "INTEGER", "mode": "REQUIRED"},

        ]

    # Set a gcp_conn_id to use a connection that you have created.
    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset", dataset_id=DATASET_NAME, gcp_conn_id='gcp_boticario')

    # create a table in a bigquery dataset
    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME,
        table_id=TB_NAME,
        schema_fields=schema_fields,
        gcp_conn_id='gcp_boticario',
    )    

    @task()
    def get_podcasts() -> pd.DataFrame:
        SEARCH_URL = f"{BASE_URL}/search?q={QUERY_PARAM}&type={TYPE}&market={MARKET}&limit={LIMIT}"

        r = req.get(SEARCH_URL, headers=headers, timeout=5)
        data = r.json()

        podcasts = {
            "id": [],
            "name": [],
            "description": [],
            "total_episodes": [],
        }

        if "error" in data.keys():
            raise AirflowException(data["error"]["message"])

        if "items" not in data["shows"].keys() or len(data["shows"]["items"]) == 0:
            raise AirflowException("No shows found")

        for show in data["shows"]["items"]:
            podcasts["id"].append(show["id"])
            podcasts["name"].append(show["name"])
            podcasts["description"].append(show["description"])
            podcasts["total_episodes"].append(show["total_episodes"])

        return podcasts

    # create a task to insert data on a postgres database
    @task()
    def load_podcasts_on_gcs(podcasts: dict):
        podcasts_df = pd.DataFrame(
            podcasts, columns=["id", "name", "description", "total_episodes"]
        )
        
        csv_buffer = StringIO()
        podcasts_df.to_csv(csv_buffer, index=False)

        gcs_hook = GCSHook(gcp_conn_id='gcp_boticario')

        gcs_hook.upload('boticario-case-2', "data_hackers/podcasts.csv", data=csv_buffer.getvalue(), mime_type= "text/csv")


    load_csv = GCSToBigQueryOperator(
        task_id='load_csv_to_bigquery',
        bucket='boticario-case-2',
        source_objects=["data_hackers/podcasts.csv"],
        destination_project_dataset_table=f'{DATASET_NAME}.{TB_NAME}',
        schema_fields=schema_fields,
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id='gcp_boticario',
    )
    
    payload = get_podcasts()
    load_on_gcs = load_podcasts_on_gcs(payload)

    load_on_gcs >> create_dataset >> create_table  >> load_csv

data_hackers_podcasts_dag = data_hackers_podcasts_dag()
