import pyspark
import minio

from spark_helper import get_spark

from delta.tables import DeltaTable

print("PySpark version: ", pyspark.__version__)
print("MinIO version: ", minio.__version__)

spark = get_spark()

METADATA_PATH = "s3a://datalake-stocks/metadata"

def create_bronze_processing_table():
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS bronze_processing_metadata(
        filename VARCHAR(256),
        processed_time TIMESTAMP
        ) USING DELTA LOCATION '{METADATA_PATH}/bronze_processing';
    """)

def reset_bronze_processing_table():
    path = f'{METADATA_PATH}/bronze_processing'
    print(f"Deleting {path}")
    DeltaTable.forPath(spark, path).delete()
    DeltaTable.forPath(spark, path).toDF().show()
    create_bronze_processing_table()

def create_silver_processing_table(delete_previous_data : bool = False):
    if delete_previous_data:
        spark.sql(f"""
            DROP TABLE IF EXISTS delta.`{METADATA_PATH}/silver_processing`;
        """)
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver_processing_metadata(
        entity_name VARCHAR(32),
        processed_time TIMESTAMP
        ) USING DELTA LOCATION '{METADATA_PATH}/silver_processing';
    """)

def create_gold_processing_table():
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold_processing_metadata(
        entity_name VARCHAR(32),
        processed_time TIMESTAMP
        ) USING DELTA LOCATION '{METADATA_PATH}/gold_processing';
    """)

def create_gold_database():
    spark.sql(f"CREATE DATABASE IF NOT EXISTS gold LOCATION '{METADATA_PATH}/gold'")