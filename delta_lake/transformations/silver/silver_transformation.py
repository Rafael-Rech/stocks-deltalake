from abc import ABC, abstractmethod
from operator import and_
from spark_helper import get_spark
from pyspark.sql.functions import (col, max, current_timestamp, to_timestamp, 
                                   round, initcap, trim, lower, upper)
from pyspark.sql import DataFrame
from datetime import datetime
import time
from delta.tables import DeltaTable

from services.storage_service import StorageService

BRONZE_PATH = "s3a://datalake-stocks/bronze"
SILVER_PATH = "s3a://datalake-stocks/silver"
METADATA_PATH = "s3a://datalake-stocks/metadata"

class SilverTransformation(ABC):
    def __init__(self, entity, partitionType):
        self.entity = entity
        self.partitionType = partitionType
        self.spark = get_spark()

    def get_last_processing_timestamp(self):
        default_timestamp = datetime(1970, 1, 1)

        # df = self.spark.table("silver_processing_metadata")\
        df = self.spark.read.format("delta").load(f"{METADATA_PATH}/silver_processing")\
        .filter(col("entity_name") == self.entity)

        row = df.first()

        if row is None:
            return default_timestamp
        
        timestamp = row["processed_time"]
        return (timestamp if timestamp is not None else default_timestamp)
        

    def read_bronze_dataframe(self):
        self.bronze_dataframe = self.spark.read.format("delta").load(f"{BRONZE_PATH}/{self.entity}")
        
        processing_timestamp = self.get_last_processing_timestamp()
        print(f"Processing timestamp: {processing_timestamp}")
        self.silver_dataframe : DataFrame = self.bronze_dataframe\
                                .filter(
                                    col("ingestion_timestamp") >= processing_timestamp
                                ).drop_duplicates()
        
    
    @abstractmethod
    def clean_data(self):
        pass

    def write_processing_timestamp(self, timestamp : datetime, start_time : float):
        self.spark.sql(f"""
                  MERGE INTO delta.`{METADATA_PATH}/silver_processing` s
                  USING (
                    SELECT '{self.entity}' as entity_name,
                    TIMESTAMP '{timestamp}' as processed_time
                  ) n
                  ON s.entity_name = n.entity_name
                  WHEN MATCHED THEN UPDATE SET s.processed_time = n.processed_time
                  WHEN NOT MATCHED THEN INSERT *
        """)

    def reset_processing_timestamp(self):
        # self.spark.sql(f"DELETE FROM delta.`{METADATA_PATH}/silver_processing` where entity_name = '{self.entity}';")
        
        delta_table = DeltaTable.forPath(self.spark, f"{METADATA_PATH}/silver_processing")
        delta_table.delete(f"entity_name = '{self.entity}'")

    @abstractmethod
    def upsert_function(self, path : str, df : DataFrame):
        pass

    def write_silver_dataframe(self, start_time : float):
        if self.silver_dataframe.limit(1).count() == 0:
            return
        
        last_bronze_ingestion_timestamp = self.silver_dataframe\
                                          .selectExpr("max(ingestion_timestamp) as max_ts")\
                                          .first()["max_ts"]


        self.write_processing_timestamp(last_bronze_ingestion_timestamp, start_time=start_time)

        dataframe = self.silver_dataframe.cache()\
                    .withColumn("ingestion_timestamp", current_timestamp())\
                    .coalesce(2)
        
        path = f"{SILVER_PATH}/{self.entity}"

        self.upsert_function(path = path, df = dataframe)
        
        # if self.partitionType == 0:
        #     # No partition
        #     dataframe.write.mode("append")\
        #         .format("delta").save(path)
        # elif self.partitionType == 1:
        #     # Partition by year
        #     dataframe.write.mode("append")\
        #         .partitionBy("year")\
        #         .format("delta").save(path)
        # elif self.partitionType == 2:
        #     # Partition by year and month
        #     dataframe.write.mode("append")\
        #         .partitionBy("year", "month")\
        #         .format("delta").save(path)

    def delete_data(self):
        # storage_service = StorageService()

        # objects = storage_service.list_objects(
        #     prefix=f"silver/{self.entity}/",
        #     recursive=True,
        # )

        # if objects is None:
        #     return
        
        # for o in objects:
        #     storage_service.remove_object(o.object_name)
        
        table_path = f"{SILVER_PATH}/{self.entity}/"
        if DeltaTable.isDeltaTable(self.spark, table_path):
            print(f"Path {table_path} is a delta table, deleting")
            DeltaTable.forPath(self.spark, table_path).delete()
            print("Table after deleting")
            # DeltaTable.forPath(self.spark, table_path).toDF().show()
        else:
            print(f"Path {table_path} is not a delta table")

    def transform(self, start_time : float, delete_previous_data : bool = False):
        if delete_previous_data:
            self.delete_data()
            self.reset_processing_timestamp()

        self.read_bronze_dataframe()
        print(f"{self.silver_dataframe.count()} rows before filtering")

        self.silver_dataframe = self.silver_dataframe\
                                .filter(col("_corrupt").isNull())\
                                .drop(col("_corrupt"))
        self.clean_data()
        self.write_silver_dataframe(start_time=start_time)

    def view_data(self):
        print(self.entity)
        self.spark.read.format("delta").load(f"{SILVER_PATH}/{self.entity}").show()

        
    
    '''
    Methods used for cleaning data,
    called by the implementations
    of this abstract class
    '''

    def remove_if_null(self, *columns : str) -> None:
        self.silver_dataframe = self.silver_dataframe.dropna(subset=columns)
        
    def convert_to_timestamp(self, column_name : str) -> None:
        self.silver_dataframe = self.silver_dataframe.withColumn(
            column_name,
            to_timestamp(col(column_name))
        )

    def round_column(self, column_name : str, scale : int = None) -> None:
        self.silver_dataframe = self.silver_dataframe.withColumn(
            column_name,
            round(col(column_name), scale)
        )

    def initcap_column(self, column_name : str) -> None:
        self.silver_dataframe = self.silver_dataframe.withColumn(
            column_name,
            initcap(trim(col(column_name)))
        )

    def lower_column(self, column_name : str) -> None:
        self.silver_dataframe = self.silver_dataframe.withColumn(
            column_name, lower(trim(col(column_name)))
        )

    def upper_column(self, column_name : str) -> None:
        self.silver_dataframe = self.silver_dataframe.withColumn(
            column_name, upper(trim(col(column_name)))
        )

    def filter_positive(self, *columns : str) -> None:
        condition = None

        for c in columns:
            expr = col(c) > 0
            condition = expr if condition is None else condition & expr

        if condition is not None:
            self.silver_dataframe = self.silver_dataframe.filter(condition)
            