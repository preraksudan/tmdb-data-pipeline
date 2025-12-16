from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

spark = SparkSession.builder \
    .appName("TMDB Analytics") \
    .getOrCreate()

CURATED = "s3a://tmdb-curated/"

cast = spark.read.format("delta").load(CURATED + "cast")

# Highest-grossing movie starring Tom Cruise
result = cast.filter(
    col("actor_name") == "Tom Cruise"
).groupBy(
    "title"
).agg(
    max("revenue").alias("revenue")
).orderBy(
    desc("revenue")
).limit(1)

result.show(truncate=False)
