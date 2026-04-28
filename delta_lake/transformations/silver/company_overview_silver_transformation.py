from delta_lake.transformations.silver.silver_transformation import SilverTransformation

from pyspark.sql import DataFrame
from pyspark.errors import AnalysisException

from delta.tables import DeltaTable
 
class CompanyOverviewSilverTransformation(SilverTransformation):
    def __init__(self):
        super().__init__("company_overview", 0)

    def clean_data(self):
        self.silver_dataframe.show()

        print()
        self.silver_dataframe.printSchema()
        print()

        self.remove_if_null("symbol")
        
        self.upper_column("symbol")
        self.initcap_column("asset_type")
        self.initcap_column("name")
        self.initcap_column("description")
        self.filter_positive("cik")
        self.upper_column("currency")
        self.upper_column("country")
        self.upper_column("sector")
        self.upper_column("industry")
        self.upper_column("address")
        self.initcap_column("fiscal_year_end")
        self.filter_positive("market_capitalization")
        self.round_column("book_value", 2)
        self.round_column("dividend_per_share", 2)
        self.round_column("eps", 2)
        self.round_column("revenue_per_share_ttm", 2)
        self.round_column("analyst_target_price", 2)


    def upsert_function(self, path : str, df : DataFrame):
        df = df.dropDuplicates(["symbol"])
        df.createOrReplaceTempView("new_data")

        if not DeltaTable.isDeltaTable(self.spark, path):
            df.write.mode("overwrite").format("delta").save(path)
            return

        try:

            self.spark.sql(f"""
            MERGE INTO delta.`{path}` t
            USING new_data AS s

            ON t.symbol = s.symbol

            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)

        except AnalysisException as e:
            # df.write.mode("append").format("delta").save(path)
            print("AnalysisException")
            raise e
