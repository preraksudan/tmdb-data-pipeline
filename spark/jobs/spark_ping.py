## This is a test file used to check .

# from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .appName("SparkPing") \
#     .getOrCreate()

# print("✅ Spark connectivity OK")

# spark.stop()

# /opt/spark-apps/jobs/spark_ping.py
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SparkPing") \
        .master("local[*]") \
        .getOrCreate()

    print("SparkSession is working!")
    df = spark.createDataFrame([(1, "Hello"), (2, "World")], ["id", "text"])
    df.show()

    spark.stop()
