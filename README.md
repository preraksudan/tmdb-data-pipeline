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


Paraqet Files visualized as json format.

## cast table
```json
  {
    "movie_id": 19995,
    "title": "Avatar",
    "cast_id": 242,
    "character": "Jake Sully",
    "credit_id": "5602a8a7c3a3685532001c9a",
    "gender": 2,
    "person_id": 65731,
    "person_name": "Sam Worthington",
    "order": 0
  }
```

## crew table
```json
  {
    "movie_id": 13576,
    "title": "This Is It",
    "credit_id": "52fe457d9251416c750583b7",
    "department": "Directing",
    "gender": 2,
    "person_id": 65310,
    "job": "Director",
    "person_name": "Kenny Ortega"
  }
```

## generes
```json
  {
    "movie_id": 19995,
    "genre_id": 28,
    "genre_name": "Action"
  }
```
## keywords
```json
  {
    "movie_id": 19995,
    "keyword_id": 1463,
    "keyword_name": "culture clash"
  }
```

## movies
```json
  {
    "budget": 237000000,
    "homepage": "http://www.avatarmovie.com/",
    "movie_id": 19995,
    "original_language": "en",
    "original_title": "Avatar",
    "overview": "In the 22nd century, a paraplegic Marine is dispatched to the moon Pandora on a unique mission, but becomes torn between following orders and protecting an alien civilization.",
    "popularity": 150.437577,
    "release_date": "2009-12-10",
    "revenue": 2787965087,
    "runtime": 162,
    "status": "Released",
    "tagline": "Enter the World of Pandora.",
    "title": "Avatar",
    "vote_average": 7.2,
    "vote_count": 11800
  }
```

##  production_companies
```json
  {
    "movie_id": 19995,
    "company_id": 289,
    "company_name": "Ingenious Film Partners"
  }
```
##  production_countries
```json
  {
    "movie_id": 19995,
    "country_code": "US",
    "country_name": "United States of America"
  }
```


## spoken_languages
```json
  {
    "movie_id": 19995,
    "language_code": "en",
    "language_name": "English"
  }
```



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
  /opt/spark-apps/jobs/analytics_queries.py