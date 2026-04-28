from delta_lake.transformations.silver.silver_transformation import SilverTransformation
from pyspark.sql.functions import col
from pyspark.sql import DataFrame

from pyspark.errors import AnalysisException
from delta.tables import DeltaTable

class SmaSilverTransformation(SilverTransformation):
    def __init__(self):
        super().__init__("simple_moving_average", 1)

    def clean_data(self):
        self.remove_if_null("symbol", "last_refreshed", "interval", "time_period",
                            "series_type", "date", "sma")

        self.upper_column("symbol")

        self.initcap_column("indicator")

        self.convert_to_timestamp("last_refreshed")

        self.lower_column("interval")

        self.silver_dataframe = self.silver_dataframe.filter(
            col("time_period") >= 0
        )

        self.lower_column("series_type")

        self.initcap_column("time_zone")

        self.convert_to_timestamp("date")

        self.round_column("sma", 4)
        self.filter_positive("sma")

    def upsert_function(self, path : str, df : DataFrame):
        df = df.dropDuplicates(["symbol", "indicator", "interval", "time_period",
                                "series_type", "date"])
        df.createOrReplaceTempView("new_data")

        if not DeltaTable.isDeltaTable(self.spark, path):
            df.write.mode("overwrite").format("delta").save(path)
            return

        try:
            self.spark.sql(f"""
            MERGE INTO delta.`{path}` t
            USING new_data AS s
            
            ON t.symbol = s.symbol AND t.indicator = s.indicator
            AND t.interval = s.interval AND t.time_period = s.time_period
            AND t.series_type = s.series_type AND t.date = s.date

            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)     

        except AnalysisException as e:
            # df.write.mode("append").format("delta").save(path)  
            print("AnalysisException")
            raise e