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

Analytics Record Format

## avg_rating_per_genre
```json
  {
  "genre_name": "History",
  "avg_rating": 6.7197969543147185
  }
```

## genre_popularity
```json
{
  "genre_name": "Drama",
  "movie_count": 2297
}
```

## tom_cruise_revenue
```json
{
  "movie_id": 956,
  "title": "Mission: Impossible III",
  "revenue": 397850012
}
```

## top_directors
```json
{
"person_name": "Sam Raimi",
"total_revenue": 3132901462,
"movie_count": 11
}
```

## top_revenue_movies
```json
{
  "movie_id": 271110,
  "title": "Captain America: Civil War",
  "revenue": 1153304495
}
```

```sh
+--------+------------------------------------+---------+
|movie_id|title                               |revenue  |
+--------+------------------------------------+---------+
|56292   |Mission: Impossible - Ghost Protocol|694713380|
|177677  |Mission: Impossible - Rogue Nation  |682330139|
|74      |War of the Worlds                   |591739379|
|955     |Mission: Impossible II              |546388105|
|954     |Mission: Impossible                 |457696359|
+--------+------------------------------------+---------+
```
only showing top 5 rows

Top 10 highest-grossing movies:
```sh
+--------+--------------------------+----------+
|movie_id|title                     |revenue   |
+--------+--------------------------+----------+
|19995   |Avatar                    |2787965087|
|597     |Titanic                   |1845034188|
|24428   |The Avengers              |1519557910|
|135397  |Jurassic World            |1513528810|
|168259  |Furious 7                 |1506249360|
|99861   |Avengers: Age of Ultron   |1405403694|
|109445  |Frozen                    |1274219009|
|68721   |Iron Man 3                |1215439994|
|211672  |Minions                   |1156730962|
|271110  |Captain America: Civil War|1153304495|
+--------+--------------------------+----------+
```

Most popular genres:

```sh
+---------------+-----------+
|genre_name     |movie_count|
+---------------+-----------+
|Drama          |2297       |
|Comedy         |1722       |
|Thriller       |1274       |
|Action         |1154       |
|Romance        |894        |
|Adventure      |790        |
|Crime          |696        |
|Science Fiction|535        |
|Horror         |519        |
|Family         |513        |
+---------------+-----------+
```

only showing top 10 rows

Top directors by total revenue:
```sh
+-----------------+-------------+-----------+
|person_name      |total_revenue|movie_count|
+-----------------+-------------+-----------+
|Steven Spielberg |9147393164   |27         |
|Peter Jackson    |6498642820   |9          |
|James Cameron    |5883569439   |7          |
|Michael Bay      |5832524638   |12         |
|Christopher Nolan|4227483234   |8          |
|Chris Columbus   |3725631503   |11         |
|Robert Zemeckis  |3590622002   |13         |
|George Lucas     |3339113893   |5          |
|Tim Burton       |3337418241   |14         |
|Ridley Scott     |3189557997   |16         |
+-----------------+-------------+-----------+
```
only showing top 10 rows

Average rating per genre:
```sh
+-----------+------------------+
|genre_name |avg_rating        |
+-----------+------------------+
|History    |6.7197969543147185|
|War        |6.713888888888889 |
|Drama      |6.388196864111485 |
|Music      |6.355675675675674 |
|Foreign    |6.352941176470588 |
|Animation  |6.341452991452987 |
|Crime      |6.274137931034488 |
|Documentary|6.238181818181816 |
|Romance    |6.207718120805376 |
|Mystery    |6.183908045977014 |
+-----------+------------------+
only showing top 10 rows
```
ALL ANALYTICS QUERIES COMPLETED SUCCESSFULLY



Airflow standalone (local setup)

docker exec -it airflow bash
airflow users list


airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

```sh
id | username | email             | first_name | last_name | roles
===+==========+===================+============+===========+======
1  | admin    | admin@example.com | Admin      | User      | Admin
```