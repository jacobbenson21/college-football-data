"""End-to-end CollegeFootballData pipeline, orchestrated by Airflow.

Each task runs one of the project's existing scripts inside the container, using
the isolated app venv (/opt/app-venv) where pyspark/dbt/etc. are installed. The
repo is mounted at /opt/airflow/project and credentials come from the container
environment (env_file in docker-compose).

Flow:
    ingest_teams ┐
                 ├─► spark_process ─► upload_to_s3 ─► load_snowflake ─► dbt_run ─► dbt_test
    ingest_games ─► ingest_stats ┘
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/airflow/project"
PY = "/opt/app-venv/bin/python"
DBT = "/opt/app-venv/bin/dbt"
SEASONS = "2022 2023 2024 2025 2026"  # 2026 is the live season (season-aware caching)

default_args = {
    "owner": "jake",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="cfb_pipeline",
    description="CollegeFootballData end-to-end pipeline: ingest -> Spark -> S3 -> Snowflake -> dbt",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["cfb"],
) as dag:

    ingest_teams = BashOperator(
        task_id="ingest_teams",
        bash_command=(
            f"cd {PROJECT} && for yr in {SEASONS}; do "
            f"{PY} ingestion/ingest_teams.py --season $yr; done"
        ),
    )

    ingest_games = BashOperator(
        task_id="ingest_games",
        bash_command=(
            f"cd {PROJECT} && for yr in {SEASONS}; do "
            f"{PY} ingestion/ingest_games.py --season $yr; done"
        ),
    )

    ingest_stats = BashOperator(
        task_id="ingest_stats",
        bash_command=(
            f"cd {PROJECT} && for yr in {SEASONS}; do "
            f"{PY} ingestion/ingest_stats.py --season $yr; done"
        ),
    )

    spark_process = BashOperator(
        task_id="spark_process",
        bash_command=(
            f"cd {PROJECT} && {PY} spark/process_teams.py "
            f"&& {PY} spark/process_games.py "
            f"&& {PY} spark/process_stats.py"
        ),
    )

    upload_to_s3 = BashOperator(
        task_id="upload_to_s3",
        bash_command=f"cd {PROJECT} && {PY} ingestion/upload_to_s3.py",
    )

    load_snowflake = BashOperator(
        task_id="load_snowflake",
        bash_command=f"cd {PROJECT} && {PY} snowflake/load_raw.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {PROJECT} && {DBT} run --project-dir dbt --profiles-dir dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {PROJECT} && {DBT} test --project-dir dbt --profiles-dir dbt",
    )

    # dependencies
    ingest_teams >> spark_process
    ingest_games >> ingest_stats >> spark_process
    spark_process >> upload_to_s3 >> load_snowflake >> dbt_run >> dbt_test
