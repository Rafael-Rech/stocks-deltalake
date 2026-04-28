from schemas.bronze_schema import BronzeSchema

from pyspark.sql.types import StringType, DateType, FloatType, IntegerType, StructField
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, to_json, from_json, explode

class TimeSeriesDailySchema(BronzeSchema):
    def __init__(self):
        super().__init__(
            [
                StructField("information", StringType(), True),
                StructField("symbol", StringType(), True),
                StructField("last_refreshed", DateType(), True),
                StructField("output_size", StringType(), True),
                StructField("time_zone", StringType(), True),
                StructField("date", DateType(), True),
                StructField("open", FloatType(), True),
                StructField("high", FloatType(), True),
                StructField("low", FloatType(), True),
                StructField("close", FloatType(), True),
                StructField("volume", IntegerType(), True)
            ]
        )

    def apply(self, raw_df : DataFrame) -> DataFrame:
        # https://stackoverflow.com/questions/69704791/convert-a-column-of-dictionary-type-to-multiple-columns-in-pyspark
        # https://stackoverflow.com/questions/70380220/convert-struct-to-map-in-spark-sql
        meta_data_keys = ["1. Information", 
                          "2. Symbol", 
                          "3. Last Refreshed", 
                          "4. Output Size", 
                          "5. Time Zone",
        ]
        meta_data_aliases = {
            meta_data_keys[0] : "information",
            meta_data_keys[1] : "symbol",
            meta_data_keys[2] : "last_refreshed",
            meta_data_keys[3] : "output_size",
            meta_data_keys[4] : "time_zone",
        }

        tsd_data_keys = [
            "1. open",
            "2. high",
            "3. low",
            "4. close",
            "5. volume",
        ]

        tsd_data_aliases = [
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]

        tsd_column = "Time Series (Daily)"
    
        df = raw_df\
            .withColumn(tsd_column, 
                from_json(to_json(col(tsd_column)),
                          'map<string, map<string, string>>')
            ).select("*", 
                *(col("Meta Data").getItem(c)\
                    .alias(meta_data_aliases.get(c)) 
                    for c in meta_data_keys),
                explode(col(tsd_column)),
                *(col("value").getItem(c)\
                    .alias(tsd_data_aliases[index]) 
                    for index, c in enumerate(tsd_data_keys))
            )\
            .withColumnRenamed("key", "date")\
            .drop(tsd_column)\
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