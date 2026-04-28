from transformations.gold.gold_transformation import GoldTransformation, GOLD_PATH

from pyspark.sql.types import StructType, StructField, DateType, ByteType, IntegerType, BooleanType
from pyspark.sql.functions import max, min, current_timestamp
from pyspark.sql import DataFrame
from pyspark.errors import AnalysisException
from datetime import datetime
from calendar import monthrange
from delta.tables import DeltaTable

class DimDateGoldTransformation(GoldTransformation):
    def __init__(self):
        super().__init__("", "dim_date", partition_type=0)
        self.MIN_YEAR = 1940
        self.MAX_YEAR = datetime.now().year + 2

    def read_silver_dataframe(self):
        self.silver_dataframe = None

    def get_schema(self) -> StructType:
        return StructType([
            StructField("date", DateType(), False),
            StructField("year", IntegerType(), False),
            StructField("month", ByteType(), False),
            StructField("day", ByteType(), False),
            StructField("week_day", ByteType(), False),
            StructField("quarter", ByteType(), False),
            StructField("is_month_end", BooleanType(), False),
            StructField("is_year_end", BooleanType(), False),
        ])
    
    def generate_new_data(self):
        data : list = []

        for year in range(self.MIN_YEAR, self.MAX_YEAR + 1):
            for month in range(1, 13):
                weekday, number_of_days = monthrange(year, month)
                for day in range(1, number_of_days + 1):
                    data.append([
                        datetime(year, month, day),
                        year,
                        month,
                        day,
                        (weekday - 1 + day) % 7,
                        ((month - 1) // 3) + 1,
                        (day == number_of_days),
                        ((day == number_of_days) and (month == 12))
                    ])

        self.gold_dataframe = self.spark.createDataFrame(data, self.get_schema())
        self.build_surrogate_key("date_sk")
        self.write_data = True

    def organize_data(self):
            # .schema(self.get_schema())\
        try:
            self.gold_dataframe = self.spark.read.format("delta")\
                                .load(f"{GOLD_PATH}/dim_date")\
                                .orderBy("year")
            
            min_value, max_value = self.gold_dataframe.select(min("year"), max("year")).first()
        except AnalysisException:
            min_value = 3000
            max_value = -3000
        if min_value is None:
            min_value = 3000
        if max_value is None:
            max_value = -3000
        if min_value > self.MIN_YEAR or max_value < self.MAX_YEAR:
            self.generate_new_data()
        else:
            self.write_data = False

    def upsert_function(self, path : str, df : DataFrame):
        # df = self.deduplicate_latest(df, ["symbol"], "latest_quarter")
        df = df.dropDuplicates(["date"])

        df.createOrReplaceTempView("new_data")

        if not DeltaTable.isDeltaTable(self.spark, path):
            df.write.mode("overwrite").format("delta").save(path)
            return

        try:
            self.spark.sql(f"""
            MERGE INTO delta.`{path}` t
            USING new_data AS s

            ON t.date = s.date

            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)
        except AnalysisException as e:
            # df.write.mode("append").format("delta").save(path)
            raise e
        
    def write_gold_dataframe(self):
        if not(self.write_data):
            return
        
        if not self.gold_dataframe.head(1):
            return

        dataframe = self.gold_dataframe.drop_duplicates() #\
                    # .withColumn("ingestion_timestamp", current_timestamp())
        
        path = f"{GOLD_PATH}/{self.gold_entity}"
        
        # dataframe.write.mode("append")\
        #     .format("delta").save(path)

        self.upsert_function(path=path, df = dataframe)

    def view_data(self):
        print(self.gold_entity)
        self.spark.read.format("delta").load(f"{GOLD_PATH}/{self.gold_entity}").orderBy("date").show()