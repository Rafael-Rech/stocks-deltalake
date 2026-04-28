from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

def get_spark() -> SparkSession :
    load_dotenv()
    spark = SparkSession.builder\
           .appName("stocks_lakehouse")\
           .config("spark.hadoop.fs.s3a.endpoint", "aistor-server:9000")\
           .config("spark.hadoop.fs.s3a.access.key", os.getenv("ACCESS_KEY"))\
           .config("spark.hadoop.fs.s3a.secret.key", os.getenv("SECRET_KEY"))\
           .config("spark.hadoop.fs.s3a.path.style.access", "true")\
           .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")\
           .config("spark.hadoop.fs.s3a.connection.timeout", "60000")\
           .config("spark.hadoop.fs.s3a.connection.maximum", "100")\
           .config("spark.hadoop.fs.s3a.threads.max", "100")\
           .config("spark.default.parallelism", "4")\
           .config("spark.sql.adaptive.enabled", "true")\
           .config("spark.sql.shuffle.partitions", 16)\
           .config("spark.sql.parquet.compression.codec", "snappy")\
           .config("spark.sql.files.maxPartitionBytes", "128MB")\
           .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark
