from schemas.bronze_schema import BronzeSchema

from pyspark.sql.types import StructField, DateType, StringType, FloatType
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, col, lit

class FederalFundsRateSchema(BronzeSchema):
    def __init__(self):
        super().__init__(
            [
                StructField("name", StringType(), True),
                StructField("interval", StringType(), True),
                StructField("unit", StringType(), True),
                StructField("date", DateType(), True),
                StructField("value", FloatType(), True)
            ]
        )

    def apply(self, raw_df : DataFrame) -> DataFrame:
        # https://stackoverflow.com/questions/69704791/convert-a-column-of-dictionary-type-to-multiple-columns-in-pyspark
        keys = ["date", "value"]

        df = raw_df.withColumn("data", explode("data"))\
                   .select("*", *(col("data").getItem(c).alias(c) for c in keys))\
                   .drop("data")
        
        struct_type = self.build_struct_type()

        df = df.select([
            col(f.name).cast(f.dataType) 
            if f.name in df.columns 
            else lit(None).cast(f.dataType).alias(f.name)
            for f in struct_type
        ])

        return df