from delta_lake.transformations.silver.silver_transformation import SilverTransformation
from pyspark.sql import DataFrame
from pyspark.errors import AnalysisException

from delta.tables import DeltaTable

class TsdSilverTransformation(SilverTransformation):
    def __init__(self):
        super().__init__("time_series_daily", 2)

    def clean_data(self):
        self.remove_if_null("symbol", "last_refreshed", "date", "open",
                            "high", "low", "close", "volume")

        self.initcap_column("information")

        self.upper_column("symbol")

        self.convert_to_timestamp("last_refreshed")

        self.initcap_column("output_size")

        self.initcap_column("time_zone")

        self.convert_to_timestamp("date")

        self.round_column("open", 2)
        self.round_column("high", 2)
        self.round_column("low", 2)
        self.round_column("close", 2)

        self.filter_positive("open", "high", "low", "close")


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
            
            ON t.symbol = s.symbol AND t.information = s.information
            AND t.date = s.date

            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)  
        except AnalysisException as e:
            # df.write.mode("append").format("delta").save(path)
            print("AnalysisException")
            raise e
                