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


# default arguments
default_args = {
    "owner": "israel siqueira",
    "start_date": days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}


@dag(
    default_args=default_args,
    schedule_interval="@hourly",
    description="Collect data hackers podcasts from spotify api and store it on a postgres database",
    catchup=False,
)
def data_hackers_podcasts_dag():
    # set variables to use in the dag tasks
    GCP_CONN_ID = "gcp_boticario"
    GCP_BUCKET = "boticario-case-2"

    DATASET_NAME = "data_hackers"
    TB_NAME = "raw_data_hackers_podcasts"

    # Set a gcp_conn_id to use a connection that you have created.
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bigquery_dataset_if_not_exists", dataset_id=DATASET_NAME, gcp_conn_id=GCP_CONN_ID
    )

    # schema for the table that will be created in bigquery
    schema_fields = [
        {"name": "id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "description", "type": "STRING", "mode": "REQUIRED"},
        {"name": "total_episodes", "type": "INTEGER", "mode": "REQUIRED"},
    ]

    # create a table in a bigquery dataset
    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_big_query_table_if_not_exists",
        dataset_id=DATASET_NAME,
        table_id=TB_NAME,
        schema_fields=schema_fields,
        gcp_conn_id=GCP_CONN_ID,
    )

    # get data hackers podcasts from spotify api
    @task()
    def get_podcasts_from_spotify_api() -> dict:

        # get spotify api token from airflow variables
        TOKEN = Variable.get("SPOTIFY_API_TOKEN")

        BASE_URL = "https://api.spotify.com/v1"  # base url for spotify api
        LIMIT = 50  # limit the number of shows returned
        MARKET = "BR"  # para limitar apenas show exibidos no Brasil
        TYPE = "show"  # limita a busca apenas a shows (podcasts)
        QUERY_PARAM = (
            "data%20hackers"  # query param to search for data hackers podcasts
        )

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {TOKEN}",
        }

        SEARCH_URL = f"{BASE_URL}/search?q={QUERY_PARAM}&type={TYPE}&market={MARKET}&limit={LIMIT}"

        # create a session to make requests
        req = requests.Session()
        r = req.get(SEARCH_URL, headers=headers, timeout=5)
        data = r.json()

        podcasts = {
            "id": [],
            "name": [],
            "description": [],
            "total_episodes": [],
        }

        # check if there is an error on the response
        if "error" in data.keys():
            raise AirflowException(data["error"]["message"])
        # check if there is any show returned on the response
        if "items" not in data["shows"].keys() or len(data["shows"]["items"]) == 0:
            raise AirflowException("No shows found")

        # filter only the fields that we need
        for show in data["shows"]["items"]:
            podcasts["id"].append(show["id"])
            podcasts["name"].append(show["name"])
            podcasts["description"].append(show["description"])
            podcasts["total_episodes"].append(show["total_episodes"])

        return podcasts

    # create a task to insert data on a gcs bucket
    @task()
    def load_podcasts_on_gcs(podcasts: dict):

        # convert the payload to a dataframe
        podcasts_df = pd.DataFrame(
            podcasts, columns=["id", "name", "description", "total_episodes"]
        )

        # convert the dataframe to csv, the buffer is used to store the csv in memory and not in the airflow container
        csv_buffer = StringIO()
        podcasts_df.to_csv(csv_buffer, index=False)

        # upload the csv to a gcs bucket
        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        gcs_hook.upload(
            GCP_BUCKET,
            "data_hackers/podcasts.csv",
            data=csv_buffer.getvalue(),
            mime_type="text/csv",
        )

    load_csv = GCSToBigQueryOperator(
        task_id="load_csv_to_bigquery",
        bucket=GCP_BUCKET,
        source_objects=["data_hackers/podcasts.csv"],
        destination_project_dataset_table=f"{DATASET_NAME}.{TB_NAME}",
        schema_fields=schema_fields,
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id=GCP_CONN_ID,
    )

    payload = get_podcasts_from_spotify_api()
    load_on_gcs = load_podcasts_on_gcs(payload)

    load_on_gcs >> create_dataset >> create_table >> load_csv


data_hackers_podcasts_dag = data_hackers_podcasts_dag()
