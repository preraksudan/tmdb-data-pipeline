from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="tmdb_lakehouse_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # manual trigger
    catchup=False,
    default_args=default_args,
    tags=["tmdb", "spark", "delta"]
) as dag:

    transform_curated = BashOperator(
        task_id="transform_tmdb_curated",
        bash_command="""
        spark-submit \
        /opt/spark/jobs/transform_tmdb.py
        """
    )

    build_analytics = BashOperator(
        task_id="build_tmdb_analytics",
        bash_command="""
        spark-submit \
        /opt/spark/jobs/analytics_queries.py
        """
    )

    transform_curated >> build_analytics
