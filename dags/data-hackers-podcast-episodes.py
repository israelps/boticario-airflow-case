# create an airflow dag with two tasks, one that create a table in a postgres database and another that insert data on the same database


from datetime import timedelta

from io import StringIO
import pandas as pd
import requests

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow import AirflowException

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.hooks.gcs import GCSHook


# default arguments for the dag
default_args = {
    "owner": "israel siqueira",
    "start_date": days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}


@dag(
    default_args=default_args,
    schedule_interval="@hourly",
    description="Collect data hackers podcast episodes from spotify api and store it on a postgres database",
    catchup=False,
)
def data_hackers_podcast_episodes_dag():

    GCP_CONN_ID = "gcp_boticario"
    GCP_BUCKET = "boticario-case-2"

    DATASET_NAME = "data_hackers"
    TB_NAME = "raw_data_hackers_episodes"
    VIEW_NAME = "vw_data_hackers_boticario_episodes"

    # Create a dataset if dont exists
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_big_query_dataset_if_not_exists",
        dataset_id=DATASET_NAME,
        gcp_conn_id=GCP_CONN_ID,
    )

    # schema for the table that will be created in bigquery
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

    # create a table in a bigquery dataset if not exists one
    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_bigquery_table_if_not_exists",
        dataset_id=DATASET_NAME,
        table_id=TB_NAME,
        schema_fields=schema_fields,
        gcp_conn_id=GCP_CONN_ID,
    )

    @task()
    def get_episodes_from_spotify_api() -> dict:
        # get all episodes from data hackers podcast

        # get the spotify api token from airflow variables
        TOKEN = Variable.get("SPOTIFY_API_TOKEN")

        BASE_URL = "https://api.spotify.com/v1"  # base url for spotify api
        MARKET = "BR"  # para limitar apenas show exibidos no Brasil
        SHOW_ID = "1oMIHOXsrLFENAeM743g93"  # ID DO GRUPO DATA HACKERS NO SPOTIFY

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer {token}".format(token=TOKEN),
        }

        SEARCH_URL = f"{BASE_URL}/shows/{SHOW_ID}/episodes?market={MARKET}"

        req = requests.Session()
        r = req.get(SEARCH_URL, headers=headers, timeout=5)
        data = r.json()

        # check if the request was successful
        if "error" in data.keys():
            raise AirflowException(data["error"]["message"])

        # check if there are shows in the response data
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
        # filter only the necessary fields
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

    # create a task to upload the csv to gcs bucket
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

        # convert the dataframe to csv, the buffer is used to store the csv in memory and not in the airflow container
        csv_buffer = StringIO()
        episodes_df.to_csv(csv_buffer, index=False)

        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        gcs_hook.upload(
            GCP_BUCKET,
            "data_hackers/podcast-episodes.csv",
            data=csv_buffer.getvalue(),
            mime_type="text/csv",
        )

    # create a task to load the csv from gcs to bigquery table
    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id="load_csv_to_bigquery",
        bucket=GCP_BUCKET,
        source_objects=["data_hackers/podcast-episodes.csv"],
        destination_project_dataset_table=f"{DATASET_NAME}.{TB_NAME}",
        schema_fields=schema_fields,
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id=GCP_CONN_ID,
    )

    # create a view with the episodes that contains the word boticario in the description field
    create_view = BigQueryCreateEmptyTableOperator(
        task_id="create_boticario_episodes_view",
        dataset_id=DATASET_NAME,
        table_id=VIEW_NAME,
        view={
            "query": f"""
            SELECT
            *
            FROM `{DATASET_NAME}.{TB_NAME}`
            WHERE REGEXP_CONTAINS(description, '(?i)(boticário|boticario)')
            """,
            "useLegacySql": False,
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    podcasts = get_episodes_from_spotify_api()
    load_on_gcs = load_episodes_on_gcs(podcasts)

    load_on_gcs >> create_dataset >> create_table >> load_csv_to_bigquery >> create_view


data_hackers_podcast_episodes_dag = data_hackers_podcast_episodes_dag()
