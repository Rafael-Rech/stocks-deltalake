from transformations.gold.gold_transformation import GoldTransformation, GOLD_PATH

from pyspark.sql import DataFrame
from pyspark.errors import AnalysisException

from delta.tables import DeltaTable

class FactTsdGoldTransformation(GoldTransformation):
    def __init__(self):
        super().__init__("time_series_daily", "fact_time_series_daily",partition_type=0)

    def organize_data(self):
        self.build_surrogate_key("tsd_sk")

        self.drop_column("output_size")
        self.drop_column("time_zone")

    def upsert_function(self, path : str, df : DataFrame):
        df = df.dropDuplicates(["symbol", "information", "date"])
        df.createOrReplaceTempView("new_data")

        if not DeltaTable.isDeltaTable(self.spark, path):
            df.write.mode("overwrite").format("delta").save(path)
            return

        try:
            self.spark.sql(f"""
            MERGE INTO delta.`{path}` t
            USING new_data AS s

            ON t.symbol = s.symbol AND
            t.information = s.information AND
            t.date = s.date

            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)      
        except AnalysisException as e:
            # df.write.mode("append").format("delta").save(path)
            raise e
        
    def view_data(self):
        print(self.gold_entity)
        self.spark.read.format("delta").load(f"{GOLD_PATH}/{self.gold_entity}").orderBy("symbol", "date").show()