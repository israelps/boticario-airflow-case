from datetime import timedelta
import pandas as pd

import os

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.hooks.gcs import GCSHook

import logging

# default arguments
default_args = {
    "owner": "israel siqueira",
    "start_date": days_ago(2),
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

@dag(
    default_args=default_args,
    schedule_interval="@hourly",
    description="Get boticario vendas data from gcs and store it on a bigquery table",
    catchup=False,
)
def boticario_vendas_dag():

    DATASET_NAME = "comercial_vendas"
    TB_NAME = "raw_vendas"
    GCP_CONN_ID = "gcp_boticario"
    GCP_BUCKET = "boticario-case-2"
    base_dir = f"{os.getcwd()}/data/raw_vendas"

    # Set a gcp_conn_id to use a connection that you have created.
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset_if_not_exists",
        dataset_id=DATASET_NAME,
        gcp_conn_id=GCP_CONN_ID,
    )

    # schema for the table that will be created in bigquery
    schema_fields = [
        {"name": "ID_MARCA", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "MARCA", "type": "STRING", "mode": "REQUIRED"},
        {"name": "ID_LINHA", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "LINHA", "type": "STRING", "mode": "REQUIRED"},
        {"name": "DATA_VENDA", "type": "DATE", "mode": "REQUIRED"},
        {"name": "QTD_VENDA", "type": "INTEGER", "mode": "REQUIRED"},
    ]

    # create a table in a bigquery dataset
    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table_if_not_exists",
        dataset_id=DATASET_NAME,
        table_id=TB_NAME,
        schema_fields=schema_fields,
        gcp_conn_id=GCP_CONN_ID,
    )

    # TODO: criar um task para fazer o upload dos arquivos para o GCS
    @task()
    def merge_files_to_csv():
        # merge all xlsx files in a csv file
        logging.info(f"base_dir: {base_dir}")
        files = os.listdir(base_dir)
        files = [file for file in files if file.endswith(".xlsx")]
        logging.info(f"files: {files}")
        vendas = []
        for file in files:
            data = pd.read_excel(f"{base_dir}/{file}")
            vendas.append(data)

        vendas_ds = pd.concat(
            vendas
        ).drop_duplicates()  # drop duplicates porque os arquivos vieram com problema (arquivos duplicados e faltando um dos anos)
        logging.info(f"vendas_ds numero de registros: {len(vendas_ds.index)}")

        target_file = f"{base_dir}/raw_vendas_consolidado.csv"
        vendas_ds.to_csv(target_file, index=False)

    DESTINATION_FILE = "raw_vendas/raw_vendas_consolidado.csv"
    upload_csv = LocalFilesystemToGCSOperator(
        task_id="upload_csv_to_gcs",
        src=f"{base_dir}/raw_vendas_consolidado.csv",
        dst=DESTINATION_FILE,
        bucket=GCP_BUCKET,
        gcp_conn_id=GCP_CONN_ID,
    )

    load_csv = GCSToBigQueryOperator(
        task_id="load_csv_to_bigquery",
        bucket=GCP_BUCKET,
        source_objects=[DESTINATION_FILE],
        destination_project_dataset_table=f"{DATASET_NAME}.{TB_NAME}",
        schema_fields=schema_fields,
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id=GCP_CONN_ID,
    )

    create_view_1 = BigQueryCreateEmptyTableOperator(
        task_id="create_view_consolidado_ano_mes",
        dataset_id=DATASET_NAME,
        table_id="vw_vendas_consolidado_ano_mes",
        view={
            "query": f"""
            SELECT 
            EXTRACT(MONTH FROM DATA_VENDA) as MES,
            EXTRACT(YEAR FROM DATA_VENDA) as ANO,
            SUM(QTD_VENDA) as VENDAS
            FROM `{DATASET_NAME}.{TB_NAME}`
            GROUP BY ANO, MES
            ORDER BY ANO, MES asc
            """,
            "useLegacySql": False,
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    create_view_2 = BigQueryCreateEmptyTableOperator(
        task_id="create_view_consolidado_marca_linha",
        dataset_id=DATASET_NAME,
        table_id="vw_vendas_consolidado_marca_linha",
        view={
            "query": f"""
            SELECT 
            MARCA,
            LINHA,
            SUM(QTD_VENDA) as VENDAS
            FROM `{DATASET_NAME}.{TB_NAME}`
            GROUP BY MARCA, LINHA
            ORDER BY MARCA, LINHA asc
            """,
            "useLegacySql": False,
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    create_view_3 = BigQueryCreateEmptyTableOperator(
        task_id="create_view_consolidado_marca_ano_mes_view",
        dataset_id=DATASET_NAME,
        table_id="vw_vendas_consolidado_marca_ano_mes",
        view={
            "query": f"""
            SELECT 
            MARCA,
            EXTRACT(MONTH FROM DATA_VENDA) as MES,
            EXTRACT(YEAR FROM DATA_VENDA) as ANO,
            SUM(QTD_VENDA) as VENDAS
            FROM `{DATASET_NAME}.{TB_NAME}`
            GROUP BY MARCA, ANO, MES
            ORDER BY MARCA, ANO, MES asc
            """,
            "useLegacySql": False,
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    create_view_4 = BigQueryCreateEmptyTableOperator(
        task_id="create_view_consolidado_linha_ano_mes_view",
        dataset_id=DATASET_NAME,
        table_id="vw_vendas_consolidado_linha_ano_mes",
        view={
            "query": f"""
            SELECT 
            LINHA,
            EXTRACT(MONTH FROM DATA_VENDA) as MES,
            EXTRACT(YEAR FROM DATA_VENDA) as ANO,
            SUM(QTD_VENDA) as VENDAS
            FROM `{DATASET_NAME}.{TB_NAME}`
            GROUP BY LINHA, ANO, MES
            ORDER BY LINHA, ANO, MES asc
            """,
            "useLegacySql": False,
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    # @task()
    # def get_data():
    #     gcs_hook = GCSHook(gcp_conn_id='gcp_boticario')
    #     files = gcs_hook.list(GCP_BUCKET, prefix="raw_vendas/", delimiter=".xlsx")
    #     vendas_ds = []
    #     for file in files:
    #         tmp_data = gcs_hook.download_as_byte_array(GCP_BUCKET, file)
    #         data = pd.read_excel(tmp_data)
    #         vendas_ds.append(data)
    #     vendas_ds = pd.concat(vendas_ds)

    merge_files = merge_files_to_csv()
    merge_files >> upload_csv >> create_dataset >> create_table >> load_csv
    load_csv >> create_view_1
    load_csv >> create_view_2
    load_csv >> create_view_3
    load_csv >> create_view_4

boticario_vendas_dag = boticario_vendas_dag()
