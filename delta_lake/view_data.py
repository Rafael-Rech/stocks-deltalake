from typing import List
from spark_helper import get_spark

from transformations.silver.ffr_silver_transformation import FfrSilverTransformation
from transformations.silver.inflation_silver_transformation import InflationSilverTransformation
from transformations.silver.sma_silver_transformation import SmaSilverTransformation
from transformations.silver.tsd_silver_transformation import TsdSilverTransformation
from transformations.silver.silver_transformation import SilverTransformation
from transformations.silver.company_overview_silver_transformation import CompanyOverviewSilverTransformation

from transformations.gold.gold_transformation import GoldTransformation
from transformations.gold.dim_company_gold_transformation import DimCompanyGoldTransformation
from transformations.gold.dim_date_gold_transformation import DimDateGoldTransformation
from transformations.gold.dim_indicator_gold_transformation import DimIndicatorGoldTransformation
from transformations.gold.fact_macro_indicator_gold_transformation import FactMacroIndicatorGoldTransformation
from transformations.gold.fact_sma_gold_transformation import FactSmaGoldTransformation
from transformations.gold.fact_tsd_gold_transformation import FactTsdGoldTransformation


from setup import create_bronze_processing_table


spark = get_spark()

LANDING_ZONE_PATH = "s3a://datalake-stocks/landing-zone/"
BRONZE_PATH = "s3a://datalake-stocks/bronze"
SILVER_PATH = "s3a://datalake-stocks/silver"
GOLD_PATH = "s3a://datalake-stocks/gold"
METADATA_PATH = "s3a://datalake-stocks/metadata"

create_bronze_processing_table()


def print_divider():
    print('=' * 30)

def view_bronze_data():
    spark.catalog.clearCache()

    print()
    print_divider()
    print("Bronze")
    entities : List[str] = ["federal_funds_rate",
        "inflation",
        "simple_moving_average",
        "time_series_daily",
        "company_overview"
    ]

    for entity in entities:
        print(entity)
        if entity in ["inflation", "federal_funds_rate"]:
            spark.read.format("delta").load(f"{BRONZE_PATH}/{entity}").orderBy("date").show()
        else:
            spark.read.format("delta").load(f"{BRONZE_PATH}/{entity}").show()

    print()
    print("Processing metadata table")
    spark.read.format("delta").load(f"{METADATA_PATH}/bronze_processing").show(n=200)
    print_divider()
    print()

def view_silver_data():
    print()
    print_divider()
    print("Silver")

    transformations : List[SilverTransformation] = [
        FfrSilverTransformation(),
        InflationSilverTransformation(),
        SmaSilverTransformation(),
        TsdSilverTransformation(),
        CompanyOverviewSilverTransformation(),
    ]

    for transformation in transformations:
        transformation.view_data()

    print("Processing metadata table")
    spark.read.format("delta").load(f"{METADATA_PATH}/silver_processing").show(n=200)

    print_divider()
    print()
    

def view_gold_data():
    print()
    print_divider()
    print("Gold")
    
    transformations : List[GoldTransformation] = [
        DimCompanyGoldTransformation(),
        DimDateGoldTransformation(),
        DimIndicatorGoldTransformation(),
        FactMacroIndicatorGoldTransformation(),
        FactSmaGoldTransformation(),
        FactTsdGoldTransformation()
    ]
    for transformation in transformations:
        transformation.view_data()

    print_divider()
    print()

def main():
    view_bronze_data()
    view_silver_data()
    view_gold_data()

if __name__ == "__main__":
    main()