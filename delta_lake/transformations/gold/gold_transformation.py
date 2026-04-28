from pyspark.sql.functions import monotonically_increasing_id, col, current_timestamp, max, row_number
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from abc import ABC, abstractmethod
from datetime import datetime

from services.storage_service import StorageService, S3Error
from spark_helper import get_spark

SILVER_PATH = "s3a://datalake-stocks/silver"
GOLD_PATH = "s3a://datalake-stocks/gold"
METADATA_PATH = "s3a://datalake-stocks/metadata"

class GoldTransformation(ABC):
    def __init__(self, original_entity : str, 
                 gold_entity : str, partition_type : int):
        self.original_entity : str = original_entity
        self.gold_entity : str = gold_entity
        self.partition_type = partition_type
        self.spark = get_spark()

    def read_silver_dataframe(self):
        self.silver_dataframe = self.spark.read.format("delta").load(f"{SILVER_PATH}/{self.original_entity}")
              
        processing_timestamp = self.get_last_processing_timestamp()
        self.gold_dataframe : DataFrame = self.silver_dataframe\
                                .filter(
                                    col("ingestion_timestamp") > processing_timestamp
                                ).drop_duplicates()
        
    def build_surrogate_key(self, sk_name : str) -> None:
        self.gold_dataframe = self.gold_dataframe.withColumn(
            sk_name,
            monotonically_increasing_id()
        )
    
    def get_last_processing_timestamp(self):
        default_timestamp = datetime(1970, 1, 1)

        # df = self.spark.table("gold_processing_metadata")\
        df = self.spark.read.format("delta").load(f"{METADATA_PATH}/gold_processing")\
        .filter(col("entity_name") == self.original_entity)

        row = df.first()

        if row is None:
            return default_timestamp
        
        timestamp = row["processed_time"]
        return (timestamp if timestamp is not None else default_timestamp)
    
    @abstractmethod
    def organize_data(self):
        pass

    def write_processing_timestamp(self, timestamp : datetime):
        self.spark.sql(f"""
                  MERGE INTO delta.`{METADATA_PATH}/gold_processing` g
                  USING (
                    SELECT '{self.original_entity}' as entity_name,
                    TIMESTAMP '{timestamp}' as processed_time
                  ) n
                  ON g.entity_name = n.entity_name
                  WHEN MATCHED THEN UPDATE SET g.processed_time = n.processed_time
                  WHEN NOT MATCHED THEN INSERT *
        """)

    @abstractmethod
    def upsert_function(self, path : str, df : DataFrame):
        pass

    def write_gold_dataframe(self) -> None:
        if not self.gold_dataframe.head(1):
            return
        
        last_silver_ingestion_timestamp = self.gold_dataframe.agg(
            max("ingestion_timestamp").alias("max_ts")
        ).first()["max_ts"]

        self.write_processing_timestamp(last_silver_ingestion_timestamp)

        dataframe = self.gold_dataframe.drop_duplicates()\
                    .withColumn("ingestion_timestamp", current_timestamp())
        
        path = f"{GOLD_PATH}/{self.gold_entity}"

        self.upsert_function(path = path, df = dataframe)


        # if self.partition_type == 0:
        #     # No partition
        #     dataframe.write.mode("append")\
        #         .format("delta").save(path)
        # elif self.partition_type == 1:
        #     # Partition by year
        #     dataframe.write.mode("append")\
        #         .partitionBy("year")\
        #         .format("delta").save(path)
        # elif self.partition_type == 2:
        #     # Partition by year and month
        #     dataframe.write.mode("append")\
        #         .partitionBy("year", "month")\
        #         .format("delta").save(path)

    def delete_data(self):
        print("Setting default processing timestamp")
        self.write_processing_timestamp(timestamp=datetime(1970,1,1))
        print("Setted default processing timestamp")
        
        table_path = f"{GOLD_PATH}/{self.gold_entity}/"
        if DeltaTable.isDeltaTable(self.spark, table_path):
            print(f"Path {table_path} is a delta table, deleting")
            DeltaTable.forPath(self.spark, table_path).delete()
            print("Table after deleting")
            DeltaTable.forPath(self.spark, table_path).toDF().show()
        else:
            print(f"Path {table_path} is not a delta table")

        # storage_service = StorageService()

        # try: 
        #     prefix=f"gold/{self.gold_entity}/"
        #     print(f"prefix = {prefix}")
        #     objects = storage_service.list_objects(
        #         prefix = prefix,
        #         recursive=True,
        #     )

        #     if objects is None:
        #         return
            
        #     for o in objects:
        #             print(o.object_name)
        #             storage_service.remove_object(o.object_name)
        # except Exception as e:
        #     print(e)

    def transform(self, delete_previous_data : bool = False):
        if delete_previous_data:
            print("Deleting data")
            self.delete_data()
        self.read_silver_dataframe()
        self.organize_data()
        self.write_gold_dataframe()

    def view_data(self):
        print(self.gold_entity)
        self.spark.read.format("delta").load(f"{GOLD_PATH}/{self.gold_entity}").show()

    def drop_column(self, column : str):
        self.gold_dataframe = self.gold_dataframe.drop(column)

    def deduplicate_latest(self, df : DataFrame, keys : list, order_col : str):
        window = Window.partitionBy(*keys).orderBy(col(order_col).desc())

        return df.withColumn("rn", row_number().over(window))\
               .filter(col("rn") == 1) \
               .drop("rn") 