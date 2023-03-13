# create an airflow dag with two tasks, one that create a table in a postgres database and another that insert data on the same database

from airflow.decorators import dag, task

from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator

from io import StringIO
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow import AirflowException
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable
import pandas as pd
import requests

req = requests.Session()

TOKEN = Variable.get("SPOTIFY_API_TOKEN")

BASE_URL = "https://api.spotify.com/v1"
MARKET = "BR"  # para limitar apenas show exibidos no Brasil
SHOW_ID = "1oMIHOXsrLFENAeM743g93"  # ID DO GRUPO DATA HACKERS NO SPOTIFY

DB_CONN_ID = "pg_boticario"
TB_NAME = "raw_data_hackers_episodes"
VIEW_NAME = "vw_data_hackers_boticario_episodes"

headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": "Bearer {token}".format(token=TOKEN),
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
    description="Collect data hackers podcast episodes from spotify api and store it on a postgres database",
)
def data_hackers_podcast_episodes_dag():
    DATASET_NAME = "data_hackers"
    TB_NAME = "raw_episodes"
    VW_NAME = "vw_boticario_episodes"

    schema_fields = [
            {"name": "id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "description", "type": "STRING", "mode": "REQUIRED"},
            {"name": "release_date", "type": "DATE", "mode": "REQUIRED"},
            {"name": "duration_ms", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "language", "type": "STRING", "mode": "REQUIRED"},
            {"name": "explicit", "type": "BOOLEAN", "mode": "REQUIRED"},
            {"name": "type", "type": "STRING", "mode": "REQUIRED"},
        ]

    # Create a dataset if dont exists
    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset_if_not_exists", dataset_id=DATASET_NAME, gcp_conn_id='gcp_boticario')

    # create a table in a bigquery dataset
    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table_if_not_exists",
        dataset_id=DATASET_NAME,
        table_id=TB_NAME,
        schema_fields=schema_fields,
        gcp_conn_id='gcp_boticario',
    )    

    @task()
    def get_episodes() -> dict:
        SEARCH_URL = (
            f"{BASE_URL}/shows/{SHOW_ID}/episodes?market={MARKET}"
        )

        r = req.get(SEARCH_URL, headers=headers, timeout=5)
        data = r.json()

        if "error" in data.keys():
            raise AirflowException(data["error"]["message"])

        if "items" not in data.keys() or len(data["items"]) == 0:
            raise AirflowException("No shows found")

        episodes = {
            "id": [],
            "name": [],
            "description": [],
            "release_date": [],
            "duration_ms": [],
            "language": [],
            "explicit": [],
            "type": [],
        }

        for episode in data["items"]:
            episodes["id"].append(episode["id"])
            episodes["name"].append(episode["name"])
            episodes["description"].append(episode["description"])
            episodes["release_date"].append(episode["release_date"])
            episodes["duration_ms"].append(episode["duration_ms"])
            episodes["language"].append(episode["language"])
            episodes["explicit"].append(episode["explicit"])
            episodes["type"].append(episode["type"])

        return episodes

    # create a task to insert data on a postgres database
    @task()
    def load_episodes_on_gcs(episodes: dict):
        episodes_df = pd.DataFrame(
            episodes,
            columns=[
                "id",
                "name",
                "description",
                "release_date",
                "duration_ms",
                "language",
                "explicit",
                "type",
            ],
        )

        csv_buffer = StringIO()
        episodes_df.to_csv(csv_buffer, index=False)

        gcs_hook = GCSHook(gcp_conn_id='gcp_boticario')

        gcs_hook.upload('boticario-case-2', "data_hackers/podcast-episodes.csv", data=csv_buffer.getvalue(), mime_type= "text/csv")

    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id='load_csv_to_bigquery',
        bucket='boticario-case-2',
        source_objects=["data_hackers/podcast-episodes.csv"],
        destination_project_dataset_table=f'{DATASET_NAME}.{TB_NAME}',
        schema_fields=schema_fields,
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id='gcp_boticario',
    )

    
    create_view = BigQueryCreateEmptyTableOperator(
        task_id="create_boticario_episodes_view",
        dataset_id=DATASET_NAME,
        table_id=VW_NAME,
        view={
            "query": f"""
            SELECT
            *
            FROM `{DATASET_NAME}.{TB_NAME}`
            WHERE REGEXP_CONTAINS(description, '(?i)(boticário|boticario)')
            """,
            "useLegacySql": False,
        },
        gcp_conn_id='gcp_boticario',
    )

       
    podcasts = get_episodes()
    load_on_gcs = load_episodes_on_gcs(podcasts)
    
    load_on_gcs >> create_dataset >> create_table >>  load_csv_to_bigquery >> create_view


data_hackers_podcast_episodes_dag = data_hackers_podcast_episodes_dag()
