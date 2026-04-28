from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
import gc
import os
from random import randint
from datetime import datetime
from typing import List

from schemas.bronze_schema import BronzeSchema
from schemas.federal_funds_rate_schema import FederalFundsRateSchema
from schemas.inflation_schema import InflationSchema
from schemas.simple_moving_average_schema import SimpleMovingAverageSchema
from schemas.time_series_daily_schema import TimeSeriesDailySchema
from schemas.company_overview_schema import CompanyOverviewSchema
from spark_helper import get_spark
from setup import create_bronze_processing_table, reset_bronze_processing_table

from services.storage_service import StorageService
from minio.commonconfig import CopySource

import time

start_time = time.time()

spark = get_spark()

LANDING_ZONE_PATH_IN = "s3a://datalake-stocks/landing-zone/process"
LANDING_ZONE_PATH_OUT = "s3a://datalake-stocks/landing-zone/processed"
BRONZE_PATH = "s3a://datalake-stocks/bronze"
METADATA_PATH = "s3a://datalake-stocks/metadata"


entities : List[str] = ["federal_funds_rate",
            "inflation",
            "simple_moving_average",
            "time_series_daily",
            "company_overview"
            ]

partitions : List[int] = [
    0, # No partition
    0, # No partition
    1, # Partition by year
    2, # Partition by year and month
    0, # No partition
]

schemas : List[BronzeSchema] = [
    FederalFundsRateSchema(),
    InflationSchema(),
    SimpleMovingAverageSchema(),
    TimeSeriesDailySchema(),
    CompanyOverviewSchema()
]

dataframes = {}

def verify_if_already_processed(object_name : str, allow : bool = False):
    df = spark.read.format("delta").load(f"{METADATA_PATH}/bronze_processing")\
        .filter(col("filename") == object_name)\
        .filter(col("processed_time").isNotNull())
    
    return (df.limit(1).count() != 0) if not(allow) else False

def write_bronze_data(df : DataFrame, partition_type : int, entity : str, overwrite : bool = False):
    global BRONZE_PATH

    df = df.dropDuplicates()\
        .withColumn("ingestion_timestamp", current_timestamp())

    path = f"{BRONZE_PATH}/{entity}"

    if partition_type == 0:
        df.write.mode("append" if not(overwrite) else "overwrite")\
            .format("delta")\
            .save(path)
    elif partition_type == 1:
        df.withColumn("year", year(col("date")))\
            .write.mode("append" if not(overwrite) else "overwrite")\
            .partitionBy("year")\
            .format("delta")\
            .save(path)
    elif partition_type == 2:
        df.withColumn("year", year(col("date")))\
        .withColumn("month", month(col("date")))\
            .write.mode("append" if not(overwrite) else "overwrite")\
            .partitionBy("year", "month")\
            .format("delta")\
            .save(path)

def set_process_timestamp(filename, reset : bool = False):
    now = datetime.now()
    reset_datetime = datetime(1970, 1, 1)

    timestamp = reset_datetime if reset else now
    timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    
    spark.sql(f"""
            MERGE INTO delta.`{METADATA_PATH}/bronze_processing` b
            USING (
                SELECT '{filename}' AS filename,
                TIMESTAMP '{timestamp_str}' AS processed_time
            ) n
            ON b.filename = n.filename
            WHEN MATCHED THEN UPDATE SET b.processed_time = n.processed_time
            WHEN NOT MATCHED THEN INSERT *
    """)


def read_landing_zone_data():
    global entities, schemas, dataframes, partitions
    global spark

    BUCKET = "s3a://datalake-stocks/"
    LANDING_ZONE_PATH = f"landing-zone/"

    storage_service = StorageService()

    for schema, entity, partition in zip(schemas, entities, partitions):
        print(entity)
        objects = storage_service.list_objects(prefix=f"{LANDING_ZONE_PATH}{entity}/")
        for o in objects:
            if not(verify_if_already_processed(o.object_name)):
                df = spark.read.option("header", "true")\
                    .option("columnNameOfCorruptRecord", "_corrupt")\
                    .option("mode", "PERMISSIVE")\
                    .json(f"{BUCKET}{o.object_name}")\
                    .withColumn("filename", input_file_name())
                
                df = schema.apply(df)

                try:
                    write_bronze_data(df, partition, entity)
                    set_process_timestamp(o.object_name)
                except Exception as e:
                    print(f"Error in file {o.object_name}: {e}")

def delete_previous_data():
    global entities

    BRONZE_PATH_WITHOUT_BUCKET = "bronze/"


    storage_service = StorageService()
    reset_bronze_processing_table()

    for entity in entities:
        objects = storage_service.list_objects(prefix=f"{BRONZE_PATH_WITHOUT_BUCKET}{entity}/", recursive=True)
        for o in objects:
            storage_service.remove_object(object_name=o.object_name)

'''
Functions that don't involve saving
processing timestamps in a table,
but instead copies and removes objects
'''
# def read_landing_zone_data():
#     global entities, schemas, dataframes
#     global LANDING_ZONE_PATH_IN#, LANDING_ZONE_PATH_OUT, BRONZE_PATH
#     global spark

#     for schema, entity in zip(schemas, entities):
#         df = spark.read.option("header", "true")\
#              .option("columnNameOfCorruptRecord", "_corrupt")\
#              .option("mode", "PERMISSIVE")\
#              .json(f"{LANDING_ZONE_PATH_IN}/{entity}")\
#              .withColumn("filename", regexp_extract("_metadata.file_path", "([^/]+$)", 0))

#         dataframes[entity] = schema.apply(df)
#         distinct_filenames = dataframes[entity].select("filename").distinct().count()
#         print("")
#         print(f"{entity} - {distinct_filenames} different files")
#         print("")

# def move_landing_zone_data():
#     global entities, schemas, dataframes
#     global LANDING_ZONE_PATH_IN, LANDING_ZONE_PATH_OUT, BRONZE_PATH
#     global spark

#     storage_service = StorageService()

#     for entity in entities:
#         in_path = f"{LANDING_ZONE_PATH_IN}/{entity}/"
#         out_path = f"{LANDING_ZONE_PATH_OUT}/{entity}/"

#         objects = storage_service.list_objects(prefix=in_path)
#         if objects is not None:
#             for o in objects:
#                 result = storage_service.copy_object(
#                     f"{out_path}{o.object_name[o.object_name.rfind("/")+1:]}",
#                     CopySource("datalake-stocks", o.object_name)
#                 )
#                 if result is not None:
#                     storage_service.remove_object(object_name=o.object_name)


# delete_previous_data()

create_bronze_processing_table()
read_landing_zone_data()

gc.collect()
del dataframes
del schemas
del entities
gc.collect()