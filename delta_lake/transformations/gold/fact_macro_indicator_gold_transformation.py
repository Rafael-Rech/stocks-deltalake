from services.storage_service import StorageService
from transformations.gold.gold_transformation import GoldTransformation, SILVER_PATH, GOLD_PATH, METADATA_PATH

from pyspark.sql.types import StructType, StructField, LongType, DateType, DoubleType, StringType
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, max
from pyspark.errors import AnalysisException
from delta.tables import DeltaTable

from datetime import datetime

class FactMacroIndicatorGoldTransformation(GoldTransformation):
    def __init__(self):
        super().__init__(original_entity="", gold_entity="fact_macro_indicator", partition_type=0)

    def get_last_processing_timestamp(self, entity_name):
        default_timestamp = datetime(1970, 1, 1)

        # df = self.spark.table("gold_processing_metadata")\
        df = self.spark.read.format("delta").load(f"{METADATA_PATH}/gold_processing")\
        .filter(col("entity_name") == entity_name)

        row = df.first()

        if row is None:
            return default_timestamp
        
        timestamp = row["processed_time"]
        return (timestamp if timestamp is not None else default_timestamp)

    def read_silver_dataframe(self):
        self.silver_dataframe = None

        processing_timestamp = self.get_last_processing_timestamp("federal_funds_rate")
        self.ffr_dataframe = self.spark.read.format("delta")\
                             .load(f"{SILVER_PATH}/federal_funds_rate")\
                             .filter(col("ingestion_timestamp") > processing_timestamp)\
                             .dropDuplicates()
        
        processing_timestamp = self.get_last_processing_timestamp("inflation")
        self.inflation_dataframe = self.spark.read.format("delta")\
                                   .load(f"{SILVER_PATH}/inflation")\
                                   .filter(col("ingestion_timestamp") > processing_timestamp)\
                                   .dropDuplicates()
        
    
    def get_schema(self) -> StructType:
        return StructType([
            # StructField("indicator_sk", LongType(), False),
            StructField("indicator_sk", StringType(), False),
            StructField("date", DateType(), False),
            StructField("value", DoubleType(), False)
        ])
    
    def organize_data(self):
        dim_indicator_dataframe = self.spark.read.format("delta")\
                                       .load(f"{GOLD_PATH}/dim_indicator")
               
        ffr_sk = dim_indicator_dataframe.filter(col("indicator_code") == "FFR")\
                 .select("indicator_sk").first()["indicator_sk"]

        inflation_sk = dim_indicator_dataframe.filter(col("indicator_code") == "Inflation")\
                 .select("indicator_sk").first()["indicator_sk"]

        ffr_dataframe = self.ffr_dataframe.select("date", "value")\
                             .withColumn("indicator_sk", lit(ffr_sk))
        
        inflation_dataframe = self.inflation_dataframe.select("date", "value")\
                             .withColumn("indicator_sk", lit(inflation_sk))
        
        schema = self.get_schema()

        self.gold_dataframe = inflation_dataframe.unionByName(ffr_dataframe)

        self.gold_dataframe = self.gold_dataframe.select(
            col("indicator_sk").cast("string"),
            col("date").cast("date"),
            col("value").cast("double")
        )

        self.build_surrogate_key("macro_indicator_sk") 

    def write_processing_timestamp(self, timestamp : datetime, entity_name : str = None):
        if entity_name is None:
            self.write_processing_timestamp(timestamp, "federal_funds_rate")
            self.write_processing_timestamp(timestamp, "inflation")
            return
        
        self.spark.sql(f"""
                  MERGE INTO delta.`{METADATA_PATH}/gold_processing` g
                  USING (
                    SELECT '{entity_name}' as entity_name,
                    TIMESTAMP '{timestamp}' as processed_time
                  ) n
                  ON g.entity_name = n.entity_name
                  WHEN MATCHED THEN UPDATE SET g.processed_time = n.processed_time
                  WHEN NOT MATCHED THEN INSERT *
        """)

    # def delete_entity_data(self, entity : str):
    #     self.write_processing_timestamp(timestamp=datetime(1970,1,1), entity_name=entity)
    #     storage_service = StorageService()

    #     objects = storage_service.list_objects(
    #         prefix=f"gold/{entity}/",
    #         recursive=True,
    #     )

    #     if objects is None:
    #         return
        
    #     for o in objects:
    #         storage_service.remove_object(o.object_name)

    # def delete_data(self):
    #     self.delete_entity_data("federal_funds_rate")
    #     self.delete_entity_data("inflation")

    def get_current_last_timestamp(self, df : DataFrame):
        return df.agg(
            max("ingestion_timestamp").alias("max_ts")
        ).first()["max_ts"]
    
    def upsert_function(self, path : str, df : DataFrame):
        df = df.dropDuplicates(["date", "indicator_sk"])
        df.groupBy("indicator_sk", "date").count().filter("count > 1").show()

        df.createOrReplaceTempView("new_data")

        if not DeltaTable.isDeltaTable(self.spark, path):
            print("Creating table")
            df.write.mode("overwrite").format("delta").save(path)
            return
        

        try:
            self.spark.sql(f"""
            MERGE INTO delta.`{path}` t
            USING new_data AS s

            ON t.date = s.date AND
            t.indicator_sk = s.indicator_sk

            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)
        except AnalysisException as e:
            # df.write.mode("append").format("delta").save(path)
            raise e
    
    def write_gold_dataframe(self):
        if not self.gold_dataframe.head(1):
            return
        
        self.write_processing_timestamp(
            self.get_current_last_timestamp(self.ffr_dataframe), 
            "federal_funds_rate",
        )
        self.write_processing_timestamp(
            self.get_current_last_timestamp(self.inflation_dataframe), 
            "inflation",
        )

        dataframe = self.gold_dataframe
        
        path = f"{GOLD_PATH}/{self.gold_entity}"

        # dataframe.write.mode("append")\
        #         .format("delta").save(path)

        self.upsert_function(path=path, df=dataframe)

    def view_data(self):        
        print(self.gold_entity)
        self.spark.read.format("delta").load(f"{GOLD_PATH}/{self.gold_entity}").orderBy("date").show()