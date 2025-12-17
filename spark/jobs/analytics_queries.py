from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, desc

# ----------------------------------
# Spark Session (Delta enabled)
# ----------------------------------
spark = SparkSession.builder \
    .appName("TMDB Analytics Queries") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

CURATED = "s3a://tmdb-curated/"
ANALYTICS = "s3a://tmdb-analytics/"

# ----------------------------------
# Read curated Delta tables
# ----------------------------------
movies = spark.read.format("delta").load(CURATED + "movies")
cast = spark.read.format("delta").load(CURATED + "cast")
crew = spark.read.format("delta").load(CURATED + "crew")
genres = spark.read.format("delta").load(CURATED + "genres")

# =====================================================
# QUERY 1: Highest-grossing movie starring Tom Cruise
# =====================================================
tom_cruise_movies = cast \
    .filter(col("person_name") == "Tom Cruise") \
    .join(movies, "movie_id") \
    .select(
        movies.movie_id,
        movies.title,
        movies.revenue
    ) \
    .orderBy(col("revenue").desc())

print("Highest-grossing movies starring Tom Cruise:")
tom_cruise_movies.show(5, truncate=False)

tom_cruise_movies.write.format("delta") \
    .mode("overwrite") \
    .save(ANALYTICS + "tom_cruise_revenue")

# =====================================================
# QUERY 2: Top 10 highest-grossing movies overall
# =====================================================
top_revenue_movies = movies \
    .select("movie_id", "title", "revenue") \
    .orderBy(col("revenue").desc()) \
    .limit(10)

print("Top 10 highest-grossing movies:")
top_revenue_movies.show(truncate=False)

top_revenue_movies.write.format("delta") \
    .mode("overwrite") \
    .save(ANALYTICS + "top_revenue_movies")

# =====================================================
# QUERY 3: Most common genres by movie count
# =====================================================
genre_popularity = genres \
    .groupBy("genre_name") \
    .agg(count("*").alias("movie_count")) \
    .orderBy(col("movie_count").desc())

print("Most popular genres:")
genre_popularity.show(10, truncate=False)

genre_popularity.write.format("delta") \
    .mode("overwrite") \
    .save(ANALYTICS + "genre_popularity")

# =====================================================
# QUERY 4: Top directors by total revenue
# =====================================================
top_directors = crew \
    .filter(col("job") == "Director") \
    .join(movies, "movie_id") \
    .groupBy("person_name") \
    .agg(
        _sum("revenue").alias("total_revenue"),
        count("movie_id").alias("movie_count")
    ) \
    .orderBy(col("total_revenue").desc())

print("Top directors by total revenue:")
top_directors.show(10, truncate=False)

top_directors.write.format("delta") \
    .mode("overwrite") \
    .save(ANALYTICS + "top_directors")

# =====================================================
# QUERY 5: Average rating per genre
# =====================================================
avg_rating_per_genre = genres \
    .join(movies, "movie_id") \
    .groupBy("genre_name") \
    .agg(avg("vote_average").alias("avg_rating")) \
    .orderBy(col("avg_rating").desc())

print("Average rating per genre:")
avg_rating_per_genre.show(10, truncate=False)

avg_rating_per_genre.write.format("delta") \
    .mode("overwrite") \
    .save(ANALYTICS + "avg_rating_per_genre")

print("ALL ANALYTICS QUERIES COMPLETED SUCCESSFULLY")

spark.stop()
