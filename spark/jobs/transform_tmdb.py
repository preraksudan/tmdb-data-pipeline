from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import (
    StructType, StructField, ArrayType,
    IntegerType, StringType, DoubleType, LongType
)

# ----------------------------------
# Spark Session (Delta enabled)
# ----------------------------------
spark = SparkSession.builder \
    .appName("TMDB Movies + Credits Curated") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

RAW = "s3a://tmdb-raw/"
CURATED = "s3a://tmdb-curated/"

# ----------------------------------
# Read RAW CSVs
# ----------------------------------
movies = spark.read \
    .option("header", True) \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv(RAW + "tmdb_5000_movies.csv")

credits = spark.read \
    .option("header", True) \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv(RAW + "tmdb_5000_credits.csv")

print("✅ Raw movies & credits loaded")

# ----------------------------------
# JSON Schemas — Movies CSV
# ----------------------------------
genres_schema = ArrayType(StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
]))

keywords_schema = ArrayType(StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
]))

production_companies_schema = ArrayType(StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
]))

production_countries_schema = ArrayType(StructType([
    StructField("iso_3166_1", StringType(), True),
    StructField("name", StringType(), True)
]))

spoken_languages_schema = ArrayType(StructType([
    StructField("iso_639_1", StringType(), True),
    StructField("name", StringType(), True)
]))

# ----------------------------------
# JSON Schemas — Credits CSV
# ----------------------------------
cast_schema = ArrayType(StructType([
    StructField("cast_id", IntegerType(), True),
    StructField("character", StringType(), True),
    StructField("credit_id", StringType(), True),
    StructField("gender", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("order", IntegerType(), True)
]))

crew_schema = ArrayType(StructType([
    StructField("credit_id", StringType(), True),
    StructField("department", StringType(), True),
    StructField("gender", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("job", StringType(), True),
    StructField("name", StringType(), True)
]))

# ----------------------------------
# Parse JSON columns — Movies
# ----------------------------------
movies_parsed = movies \
    .withColumn("genres", from_json(col("genres"), genres_schema)) \
    .withColumn("keywords", from_json(col("keywords"), keywords_schema)) \
    .withColumn("production_companies", from_json(col("production_companies"), production_companies_schema)) \
    .withColumn("production_countries", from_json(col("production_countries"), production_countries_schema)) \
    .withColumn("spoken_languages", from_json(col("spoken_languages"), spoken_languages_schema))

# ----------------------------------
# Parse JSON columns — Credits
# ----------------------------------
credits_parsed = credits \
    .withColumn("cast", from_json(col("cast"), cast_schema)) \
    .withColumn("crew", from_json(col("crew"), crew_schema))

# ----------------------------------
# CORE MOVIES TABLE
# ----------------------------------
movies_df = movies_parsed.select(
    col("budget").cast("int"),
    col("homepage"),
    col("id").cast("int").alias("movie_id"),
    col("original_language"),
    col("original_title"),
    col("overview"),
    col("popularity").cast("double"),
    col("release_date"),
    col("revenue").cast("long"),
    col("runtime").cast("int"),
    col("status"),
    col("tagline"),
    col("title"),
    col("vote_average").cast("double"),
    col("vote_count").cast("int")
)

# ----------------------------------
# DIMENSION TABLES — Movies
# ----------------------------------
genres_df = movies_parsed \
    .select(col("id").cast("int").alias("movie_id"), explode("genres").alias("g")) \
    .select(
        "movie_id",
        col("g.id").alias("genre_id"),
        col("g.name").alias("genre_name")
    )

keywords_df = movies_parsed \
    .select(col("id").cast("int").alias("movie_id"), explode("keywords").alias("k")) \
    .select(
        "movie_id",
        col("k.id").alias("keyword_id"),
        col("k.name").alias("keyword_name")
    )

production_companies_df = movies_parsed \
    .select(col("id").cast("int").alias("movie_id"), explode("production_companies").alias("pc")) \
    .select(
        "movie_id",
        col("pc.id").alias("company_id"),
        col("pc.name").alias("company_name")
    )

production_countries_df = movies_parsed \
    .select(col("id").cast("int").alias("movie_id"), explode("production_countries").alias("pc")) \
    .select(
        "movie_id",
        col("pc.iso_3166_1").alias("country_code"),
        col("pc.name").alias("country_name")
    )

spoken_languages_df = movies_parsed \
    .select(col("id").cast("int").alias("movie_id"), explode("spoken_languages").alias("sl")) \
    .select(
        "movie_id",
        col("sl.iso_639_1").alias("language_code"),
        col("sl.name").alias("language_name")
    )

# ----------------------------------
# CAST TABLE
# ----------------------------------
cast_df = credits_parsed \
    .select(
        col("movie_id").cast("int").alias("movie_id"),
        col("title"),
        explode("cast").alias("c")
    ) \
    .select(
        "movie_id",
        "title",
        col("c.cast_id"),
        col("c.character"),
        col("c.credit_id"),
        col("c.gender"),
        col("c.id").alias("person_id"),
        col("c.name").alias("person_name"),
        col("c.order")
    )

# ----------------------------------
# CREW TABLE
# ----------------------------------
crew_df = credits_parsed \
    .select(
        col("movie_id").cast("int").alias("movie_id"),
        col("title"),
        explode("crew").alias("c")
    ) \
    .select(
        "movie_id",
        "title",
        col("c.credit_id"),
        col("c.department"),
        col("c.gender"),
        col("c.id").alias("person_id"),
        col("c.job"),
        col("c.name").alias("person_name")
    )

# ----------------------------------
# WRITE DELTA TABLES — MOVIES
# ----------------------------------
movies_df.write.format("delta").mode("overwrite").save(CURATED + "movies")
genres_df.write.format("delta").mode("overwrite").save(CURATED + "genres")
keywords_df.write.format("delta").mode("overwrite").save(CURATED + "keywords")
production_companies_df.write.format("delta").mode("overwrite").save(CURATED + "production_companies")
production_countries_df.write.format("delta").mode("overwrite").save(CURATED + "production_countries")
spoken_languages_df.write.format("delta").mode("overwrite").save(CURATED + "spoken_languages")

print("✅ MOVIES TABLES WRITTEN")

# ----------------------------------
# WRITE DELTA TABLES — CREDITS
# ----------------------------------
cast_df.write.format("delta").mode("overwrite").save(CURATED + "cast")
crew_df.write.format("delta").mode("overwrite").save(CURATED + "crew")

print("✅ CREDITS TABLES WRITTEN")
print("🎉 TMDB CURATED LAYER COMPLETED SUCCESSFULLY")

spark.stop()
