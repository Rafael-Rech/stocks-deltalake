from schemas.bronze_schema import BronzeSchema

from pyspark.sql.types import StructField, StringType, DateType, IntegerType, FloatType
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, col, lit, from_json, to_json

class SimpleMovingAverageSchema(BronzeSchema):
    def __init__(self):
        super().__init__(
            [
                StructField("symbol", StringType(), True),
                StructField("indicator", StringType(), True),
                StructField("last_refreshed", DateType(), True),
                StructField("interval", StringType(), True),
                StructField("time_period", IntegerType(), True),
                StructField("series_type", StringType(), True),
                StructField("time_zone", StringType(), True),
                StructField("date", DateType(), True),
                StructField("sma", FloatType(), True)
            ]
        )

    
    def apply(self, raw_df : DataFrame) -> DataFrame:
        # https://stackoverflow.com/questions/69704791/convert-a-column-of-dictionary-type-to-multiple-columns-in-pyspark
        # https://stackoverflow.com/questions/70380220/convert-struct-to-map-in-spark-sql
        keys = ["1: Symbol", "2: Indicator", "3: Last Refreshed", "4: Interval",
                "5: Time Period", "6: Series Type", "7: Time Zone"]
        aliases = {
            keys[0] : "symbol",
            keys[1] : "indicator",
            keys[2] : "last_refreshed",
            keys[3] : "interval",
            keys[4] : "time_period",
            keys[5] : "series_type",
            keys[6] : "time_zone",
        }
    
        df = raw_df\
            .withColumn("Technical Analysis: SMA", 
                from_json(to_json(col("Technical Analysis: SMA")),
                          'map<string, map<string, string>>')
            ).select("*", 
                *(col("Meta Data").getItem(c)\
                    .alias(aliases.get(c)) 
                    for c in keys),
                explode(col("Technical Analysis: SMA")),
                *(col("value").getItem(c)\
                    .alias("sma") 
                    for c in ["SMA"])
            )\
            .withColumnRenamed("key", "date")\
            .drop("Technical Analysis: SMA")\
            .drop("Meta Data")\
            .drop("value")

        struct_type = self.build_struct_type()

        df = df.select([
            col(f.name).cast(f.dataType) 
            if f.name in df.columns 
            else lit(None).cast(f.dataType).alias(f.name)
            for f in struct_type
        ])
        
        return df