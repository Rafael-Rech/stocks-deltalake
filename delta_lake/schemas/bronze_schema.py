from pyspark.sql.types import StructField, StringType, StructType
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class BronzeSchema(ABC):
    def __init__(self, fields : list):
        self.fields = fields + [StructField("_corrupt", StringType(), True),
                                StructField("filename", StringType(), True)]

    def build_struct_type(self) -> StructType:
        return StructType(self.fields)
    
    @abstractmethod
    def apply(self, raw_df : DataFrame) -> DataFrame:
        pass