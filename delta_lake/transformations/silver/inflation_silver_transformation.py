from delta_lake.transformations.silver.silver_transformation import SilverTransformation, SILVER_PATH

from pyspark.sql import DataFrame
from pyspark.errors import AnalysisException
from delta.tables import DeltaTable

class InflationSilverTransformation(SilverTransformation):
    def __init__(self):
        super().__init__("inflation", 0)

    def clean_data(self):
        self.remove_if_null("interval", "unit", "date", "value")

        self.initcap_column("name")

        self.lower_column("interval")
        
        self.lower_column("unit")

        self.convert_to_timestamp(column_name="date")

        self.round_column("value", 13)

    def upsert_function(self, path : str, df : DataFrame):
        df = df.dropDuplicates(["name", "interval", "unit", "date"])
        df.createOrReplaceTempView("new_data")

        if not DeltaTable.isDeltaTable(self.spark, path):
            df.write.mode("overwrite").format("delta").save(path)
            return

        try:
            self.spark.sql(f"""
            MERGE INTO delta.`{path}` t
            USING new_data AS s
            
            ON t.name = s.name AND t.interval = s.interval
            AND t.unit = s.unit AND t.date = s.date

            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)   
        except AnalysisException as e:
            # df.write.mode("append").format("delta").save(path)
            print("AnalysisException")
            raise e
    
    def view_data(self):
        print(self.entity)
        self.spark.read.format("delta").load(f"{SILVER_PATH}/{self.entity}").orderBy("date").show()