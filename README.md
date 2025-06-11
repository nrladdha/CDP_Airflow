# CDP DataHub Airflow Operator

This repo provides an Airflow operator for managing CDP DataHub clusters. It is based on CDP CLI for interacting with CDP datahub cluster.
Hence has dependecy on installing cdp cli

## Installation

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

2. Make sure you have the CDP CLI installed and configured with your credentials.

## Usage

The [sample_cdp_datahub_dag.py] (https://github.com/nrladdha/CDP_Airflow/blob/main/dags/sample_cdp_datahub_dag.py) from dags folder provides example of how to use the operator in your Airflow DAG. It supports two operations - start & stop data hub cluster.

Typical Airflow DAG will add task using CDPDataHubOperator to start datahub cluster, then execute required jobs in the cluster (note this repo does not include operator for job execution - .eg. Spark-LiveOperator, you can use existing operators). Once job is finished then add task using same CDPDataHubOperator to stop the datahub cluster.



## Parameters

- `cluster_name` (required): Name of the CDP DataHub cluster
- `environment_name` (required): CDP environment name
- `operation` (optional): The operation to perform ('start' or 'stop', default: 'start')
- `wait_for_cluster` (optional): Whether to wait for cluster to be ready before proceeding (default: True)
- `cluster_wait_timeout` (optional): Timeout in seconds for waiting cluster to be ready (default: 1800)

## Features

1. Start / Stop CDP DataHub cluster
2. Wait for cluster to be ready (optional)
3. Configurable timeouts

## Requirements

- Python 3.7+
- Apache Airflow 2.5.0+
- CDP CLI installed and configured
- Proper permissions to manage CDP DataHub clusters 