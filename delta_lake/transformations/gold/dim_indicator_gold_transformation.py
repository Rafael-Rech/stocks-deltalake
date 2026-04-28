from transformations.gold.gold_transformation import GoldTransformation, SILVER_PATH, GOLD_PATH

from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql import DataFrame
from pyspark.sql.functions import sha2, concat_ws, col
from pyspark.errors import AnalysisException
from delta.tables import DeltaTable

class DimIndicatorGoldTransformation(GoldTransformation):
    def __init__(self):
        super().__init__(original_entity="", gold_entity="dim_indicator", partition_type=0)


    def read_silver_dataframe(self):
        self.silver_dataframe = None

        self.ffr_dataframe = self.spark.read.format("delta")\
                             .load(f"{SILVER_PATH}/federal_funds_rate")
        
        self.inflation_dataframe = self.spark.read.format("delta")\
                                   .load(f"{SILVER_PATH}/inflation")
        
    def get_schema(self) -> StructType:
        return StructType([
            StructField("indicator_code", StringType(), False),
            StructField("name", StringType(), False),
            StructField("interval", StringType(), False),
            StructField("unit", StringType(), False)
        ])
    
    def extract_data(self, df : DataFrame, indicator_code : str):
        row = df.first()

        if row is None:
            raise ValueError("No row found in silver dataframe")
        
        name = row["name"]
        interval = row["interval"]
        unit = row["unit"]

        return [indicator_code, name, interval, unit]
    
    def build_surrogate_key(self, sk_name):
        self.gold_dataframe = self.gold_dataframe.withColumn(
            sk_name,
            sha2(concat_ws("||", col("indicator_code"), col("name")), 256)   
        )

    def organize_data(self):
        data = [
            self.extract_data(self.ffr_dataframe, "FFR"),
            self.extract_data(self.inflation_dataframe, "Inflation"),
        ]
        schema = self.get_schema()

        self.gold_dataframe = self.spark.createDataFrame(
            data = data,
            schema = schema
        )

        self.build_surrogate_key("indicator_sk")

    def upsert_function(self, path : str, df : DataFrame):
        df = df.dropDuplicates(["name", "interval", "unit"])

        df.createOrReplaceTempView("new_data")

        if not DeltaTable.isDeltaTable(self.spark, path):
            df.write.mode("overwrite").format("delta").save(path)
            return

        try:
            self.spark.sql(f"""
            MERGE INTO delta.`{path}` t
            USING new_data AS s

            ON t.name = s.name AND
            t.interval = s.interval AND
            t.unit = s.unit

            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)
        except AnalysisException as e:
            # df.write.mode("append").format("delta").save(path)
            raise e
        
    def write_gold_dataframe(self):
        if not self.gold_dataframe.head(1):
            return

        dataframe = self.gold_dataframe
        
        path = f"{GOLD_PATH}/{self.gold_entity}"

        self.upsert_function(path=path, df=dataframe)

        # dataframe.write.mode("overwrite")\
        #         .format("delta").save(path)

        # try:
        #     delta_table = DeltaTable.forPath(self.spark, path)

        #     merge_condition = ""
        #     (
        #         delta_table.alias("target")
        #         .merge(
        #             dataframe.alias("source"),
        #             merge_condition
        #         )
        #         .whenMatchedUpdateAll()
        #         .whenNotMatchedInsertAll()
        #         .execute()
        #     )

        # except AnalysisException:
        #     dataframe.write.mode("overwrite").format("delta").save(path)