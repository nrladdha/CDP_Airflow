from __future__ import annotations
from datetime import datetime
from airflow.sdk import DAG, Label
from custom_operators.cdp_datahub_operator import CDPDataHubOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

with DAG(
    "sample_cdp_datahub_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:
    start_cluster = CDPDataHubOperator(
        task_id='start_cluster',
        cluster_name='nl-load-iceberg',
        environment_name='se-sandboxx-aws',
        operation='start',
        wait_for_cluster=True,
        cluster_wait_timeout=1800  
    )

    stop_cluster = CDPDataHubOperator(
        task_id='stop_cluster',
        cluster_name='nl-load-iceberg',
        environment_name='se-sandboxx-aws',
        operation='stop',
        wait_for_cluster=True,
        cluster_wait_timeout=1800 
    )

    start_cluster