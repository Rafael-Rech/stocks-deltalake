from transformations.gold.gold_transformation import GoldTransformation, GOLD_PATH

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

class FactSmaGoldTransformation(GoldTransformation):
    def __init__(self):
        super().__init__("simple_moving_average", "fact_simple_moving_average",partition_type=0)

    def organize_data(self):
        self.build_surrogate_key("sma_sk")

        self.drop_column("time_zone")

    def upsert_function(self, path : str, df : DataFrame):
        df = df.dropDuplicates(subset=["symbol",
                                       "indicator",
                                       "interval",
                                       "time_period",
                                       "series_type",
                                       "date"])
    
        df.createOrReplaceTempView("new_data")

        if not DeltaTable.isDeltaTable(self.spark, path):
            df.write.mode("overwrite").format("delta").save(path)
            return

        try:
            self.spark.sql(f"""
            MERGE INTO delta.`{path}` t
            USING new_data AS s

            ON t.symbol = s.symbol AND
            t.indicator = s.indicator AND
            t.interval = s.interval AND
            t.time_period = s.time_period AND
            t.series_type = s.series_type AND
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