from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType, StringType

# ----------------------------------
# Spark Session (Delta enabled)
# ----------------------------------
spark = SparkSession.builder \
    .appName("TMDB Transform Exploded") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ----------------------------------
# Paths
# ----------------------------------
RAW = "s3a://tmdb-raw/"
CURATED = "s3a://tmdb-curated/"

# ----------------------------------
# Read credits CSV
# ----------------------------------
credits = spark.read \
    .option("header", True) \
    .option("delimiter", ",") \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv(RAW + "tmdb_5000_credits.csv")

print("Raw credits loaded")
credits.show(1, truncate=100)

# ----------------------------------
# Schemas for JSON arrays
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
# Parse JSON arrays
# ----------------------------------
credits_parsed = credits \
    .withColumn("cast", from_json(col("cast"), cast_schema)) \
    .withColumn("crew", from_json(col("crew"), crew_schema))

# ----------------------------------
# Explode cast array into rows
# ----------------------------------
cast_df = credits_parsed \
    .select(
        col("movie_id"),
        col("title"),
        explode("cast").alias("cast_member")
    ) \
    .select(
        "movie_id",
        "title",
        col("cast_member.cast_id"),
        col("cast_member.character"),
        col("cast_member.credit_id"),
        col("cast_member.gender"),
        col("cast_member.id"),
        col("cast_member.name"),
        col("cast_member.order")
    )

# ----------------------------------
# Explode crew array into rows
# ----------------------------------
crew_df = credits_parsed \
    .select(
        col("movie_id"),
        col("title"),
        explode("crew").alias("crew_member")
    ) \
    .select(
        "movie_id",
        "title",
        col("crew_member.credit_id"),
        col("crew_member.department"),
        col("crew_member.gender"),
        col("crew_member.id"),
        col("crew_member.job"),
        col("crew_member.name")
    )

# ----------------------------------
# Show previews (optional)
# ----------------------------------
print("Preview of exploded cast DataFrame:")
cast_df.show(5, truncate=False)
print("Preview of exploded crew DataFrame:")
crew_df.show(5, truncate=False)

# ----------------------------------
# Write Delta tables
# ----------------------------------
cast_df.write.format("delta") \
    .mode("overwrite") \
    .save(CURATED + "cast")

crew_df.write.format("delta") \
    .mode("overwrite") \
    .save(CURATED + "crew")

print("DELTA WRITES COMPLETED SUCCESSFULLY")

spark.stop()
