### Sourcing data from .zip files to .csv files into un-compressed format
/raw_dataset
  - contains raw_dataset/tmdb_5000_credits.csv.zip
  - contains raw_dataset/tmdb_5000_movies.csv.zip
  on running unzip.py it un-zips and moves to /unzipped folder as root directory

### Creating Object Storage Buckets of the .csv files on minio (configured on port 9001) accessible via http://localhost:9001/
  - run ```sh script ./scripts/upload_to_minio.sh``` at the root directory
  - later on as enhancement (Airflow task) or a dataloader can be used

## Deliverable 1 : Datasets present and verified in MinIO. 
  - run command : ```sh mc ls local/tmdb-raw``` to verify existance of object stores in minio local

---

# Read from MinIO with Spark

Configure Spark to read the CSV files directly from MinIO (S3-compatible endpoint).

Handle schema inference or explicit schema definitions as needed.

Deliverable: Spark job(s) that reliably read both datasets from MinIO.

## Final Working version in spark job execution

after running 

- docker exec -it spark bash

/opt/spark/bin/spark-submit \
  --master local[*] \
  --conf spark.jars.ivy=/opt/spark/ivy \
  --packages \
io.delta:delta-spark_2.12:3.1.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  /opt/spark-apps/jobs/transform_tmdb.py

Testing readme