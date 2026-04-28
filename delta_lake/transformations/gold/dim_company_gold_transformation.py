from transformations.gold.gold_transformation import GoldTransformation, GOLD_PATH

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

class DimCompanyGoldTransformation(GoldTransformation):
    def __init__(self):
        super().__init__("company_overview", "dim_company", partition_type=0)

    def organize_data(self):
        # self.gold_dataframe.show(n = 50, truncate = 25)

        self.build_surrogate_key(sk_name="company_sk")

        self.drop_column("description")
        self.drop_column("address")
        self.drop_column("official_site")
        self.drop_column("cik")
        self.drop_column("trailing_pe")
        self.drop_column("peg_ratio")
        self.drop_column("ev_to_revenue")
        self.drop_column("diluted_eps_ttm")
        self.drop_column("return_on_assets_ttm")
        self.drop_column("dividend_date")
        self.drop_column("ex_dividend_date")
        self.drop_column("52_week_high")
        self.drop_column("52_week_low")
        self.drop_column("50_day_moving_average")
        self.drop_column("200_day_moving_average")
        self.drop_column("analyst_rating_strong_buy")
        self.drop_column("analyst_rating_buy")
        self.drop_column("analyst_rating_hold")
        self.drop_column("analyst_rating_sell")
        self.drop_column("analyst_rating_strong_sell")

    def upsert_function(self, path : str, df : DataFrame):
        # df = df.dropDuplicates(subset=["symbol"])
        df = self.deduplicate_latest(df, ["symbol"], "latest_quarter")

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
            raise e
        
    def view_data(self):
        print(self.gold_entity)
        self.spark.read.format("delta").load(f"{GOLD_PATH}/{self.gold_entity}").orderBy("symbol").show()