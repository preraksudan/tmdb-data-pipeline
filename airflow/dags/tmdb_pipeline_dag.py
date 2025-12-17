"""
Airflow DAG: TMDB Data Pipeline
Author: Your Name
Date: YYYY-MM-DD

Description:
This DAG orchestrates the TMDB ETL pipeline:
1. Uploads raw CSV files to MinIO (`tmdb-raw`)
2. Checks that files exist in MinIO
3. Runs Spark transformation jobs to generate curated Delta tables
4. Executes example analytics queries on Delta tables

Pre-requisites:
- MinIO running with buckets: tmdb-raw, tmdb-curated
- Spark container accessible
- Delta Lake configured in Spark
- .env file contains MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, MINIO_ENDPOINT
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
import os
import logging
import boto3
from botocore.exceptions import ClientError

# -----------------------------
# DAG default arguments
# -----------------------------
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 2,  # retry on failure
    'retry_delay': 300  # 5 minutes
}

dag = DAG(
    'tmdb_pipeline',
    default_args=default_args,
    description='ETL pipeline for TMDB data (raw -> curated -> analytics)',
    schedule_interval=None,  # run manually
    catchup=False
)

# -----------------------------
# TASK 1: Upload raw CSVs to MinIO
# -----------------------------
def upload_to_minio(bucket_name, files):
    """
    Upload local CSV files to MinIO and log each step.
    """
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ROOT_USER")
    secret_key = os.environ.get("MINIO_ROOT_PASSWORD")

    logging.info(f"Connecting to MinIO at {minio_endpoint}")
    s3 = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    # Check if bucket exists; create if not
    try:
        s3.head_bucket(Bucket=bucket_name)
        logging.info(f"Bucket {bucket_name} already exists")
    except ClientError:
        s3.create_bucket(Bucket=bucket_name)
        logging.info(f"Created bucket {bucket_name}")

    # Upload files
    for f in files:
        if os.path.exists(f):
            s3.upload_file(f, bucket_name, os.path.basename(f))
            logging.info(f"Uploaded {f} -> s3://{bucket_name}/{os.path.basename(f)}")
        else:
            logging.error(f"File not found: {f}")
            raise FileNotFoundError(f"File not found: {f}")

upload_raw_files = PythonOperator(
    task_id='upload_raw_files',
    python_callable=upload_to_minio,
    op_kwargs={
        'bucket_name': 'tmdb-raw',
        'files': [
            '/opt/data/raw/tmdb_5000_movies.csv',
            '/opt/data/raw/tmdb_5000_credits.csv'
        ]
    },
    dag=dag
)

# -----------------------------
# TASK 2: Check raw files in MinIO
# -----------------------------
def check_raw_files(bucket_name, files):
    """
    Validate that required files exist in MinIO, log results.
    """
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ROOT_USER")
    secret_key = os.environ.get("MINIO_ROOT_PASSWORD")

    s3 = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    missing_files = []
    for f in files:
        try:
            s3.head_object(Bucket=bucket_name, Key=os.path.basename(f))
            logging.info(f"Found file in bucket: {f}")
        except ClientError:
            logging.warning(f"Missing file in bucket: {f}")
            missing_files.append(f)

    if missing_files:
        logging.error(f"Missing files: {missing_files}")
        raise FileNotFoundError(f"Missing files: {missing_files}")

check_files = PythonOperator(
    task_id='check_raw_files',
    python_callable=check_raw_files,
    op_kwargs={
        'bucket_name': 'tmdb-raw',
        'files': [
            '/opt/data/raw/tmdb_5000_movies.csv',
            '/opt/data/raw/tmdb_5000_credits.csv'
        ]
    },
    dag=dag
)



def create_bucket_if_missing(bucket_name):
    import os
    import boto3
    from botocore.exceptions import ClientError

    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ROOT_USER")
    secret_key = os.environ.get("MINIO_ROOT_PASSWORD")

    s3 = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} already exists")
    except ClientError:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Created bucket {bucket_name}")




# -----------------------------
# TASK 3: Create curated bucket
# ----------------------------- 

# Airflow tasks
create_curated_bucket = PythonOperator(
    task_id="create_curated_bucket",
    python_callable=create_bucket_if_missing,
    op_kwargs={"bucket_name": "tmdb-curated"},
    dag=dag
)


debug_spark_paths = BashOperator(
    task_id="debug_spark_paths",
    bash_command="""
    docker exec spark bash -c '
      echo "Spark submit location:"
      which spark-submit
      echo "Jobs directory:"
      ls -lh /opt/spark-apps/jobs
    '
    """,
    dag=dag,
)


# -----------------------------
# TASK 4: Spark transformations
# ----------------------------- 
spark_transform = BashOperator(
    task_id='spark_transform',
    bash_command="""
    docker exec -i spark /opt/spark/bin/spark-submit \
      --master local[*] \
      --conf spark.jars.ivy=/opt/spark/ivy \
      --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
      --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
      --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
      --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
      --conf spark.hadoop.fs.s3a.access.key=minioadmin \
      --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      /opt/spark-apps/jobs/transform_tmdb.py
    """,
    dag=dag
)


# docker exec -i spark /opt/spark/bin/spark-submit \
#   --master local[*] \
#   --conf spark.jars.ivy=/opt/spark/ivy \
#   --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
#   --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
#   --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
#   --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
#   --conf spark.hadoop.fs.s3a.access.key=minioadmin \
#   --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
#   --conf spark.hadoop.fs.s3a.path.style.access=true \
#   /opt/spark-apps/jobs/transform_tmdb.py



# -----------------------------
# TASK 6: Create analytics bucket
# -----------------------------

create_analytics_bucket = PythonOperator(
    task_id="create_analytics_bucket",
    python_callable=create_bucket_if_missing,
    op_kwargs={"bucket_name": "tmdb-analytics"},
    dag=dag
)


# -----------------------------
# TASK 7: Analytics queries
# -----------------------------
spark_analytics = BashOperator(
    task_id='spark_analytics_queries',
    bash_command="""
    docker exec -i spark /opt/spark/bin/spark-submit \
      --master local[*] \
      --conf spark.jars.ivy=/opt/spark/ivy \
      --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
      --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
      --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
      --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
      --conf spark.hadoop.fs.s3a.access.key=${MINIO_ROOT_USER} \
      --conf spark.hadoop.fs.s3a.secret.key=${MINIO_ROOT_PASSWORD} \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      /opt/spark-apps/jobs/analytics_queries.py
    """,
    dag=dag
)


# -----------------------------
# Task sequence
# -----------------------------
upload_raw_files >> check_files >> create_curated_bucket >> debug_spark_paths >> spark_transform >> create_analytics_bucket >> spark_analytics


# # Commenting to test another code snippet first
# # /opt/airflow/dags/spark_ping_test_dag.py
# from airflow import DAG
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from datetime import datetime

# default_args = {
#     "owner": "airflow",
#     "retries": 1,
# }

# with DAG(
#     dag_id="spark_ping_test",
#     start_date=datetime(2025, 1, 1),
#     schedule_interval=None,
#     catchup=False,
#     default_args=default_args,
# ) as dag:

#     spark_ping = SparkSubmitOperator(
#         task_id="spark_ping",
#         application="/opt/spark-apps/jobs/spark_ping.py",  # path inside container
#         conn_id="spark_default",  # ensure this exists in Airflow Connections
#         name="spark-ping",
#         verbose=True,
#         master="local[*]",  # optional override
#     )
#     spark_ping