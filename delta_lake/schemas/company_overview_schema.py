from schemas.bronze_schema import BronzeSchema

from pyspark.sql.types import StringType, DateType, FloatType, IntegerType, StructField, LongType, ByteType
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

class CompanyOverviewSchema(BronzeSchema):
    
    def __init__(self):        
        super().__init__(
            [
                StructField("symbol", StringType(), True),
                StructField("asset_type", StringType(), True),
                StructField("name" , StringType(), True),
                StructField("description", StringType(), True),
                StructField("cik", IntegerType(), True),
                StructField("exchange", StringType(), True),
                StructField("currency", StringType(), True),
                StructField("country", StringType(), True),
                StructField("sector", StringType(), True),
                StructField("industry" , StringType(), True),
                StructField("address" , StringType(), True),
                StructField("official_site", StringType(), True),
                StructField("fiscal_year_end", StringType(), True),
                StructField("latest_quarter", DateType(), True),
                StructField("market_capitalization", LongType(), True),
                StructField("ebitda", LongType(), True),
                StructField("pe_ratio", FloatType(), True),
                StructField("peg_ratio", FloatType(), True),
                StructField("book_value", FloatType(), True),
                StructField("dividend_per_share", FloatType(), True),
                StructField("dividend_yield", FloatType(), True),
                StructField("eps", FloatType(), True),
                StructField("revenue_per_share_ttm", FloatType(), True),
                StructField("profit_margin", FloatType(), True),
                StructField("operating_margin_ttm", FloatType(), True),
                StructField("return_on_assets_ttm", FloatType(), True),
                StructField("return_on_equity_ttm", FloatType(), True),
                StructField("revenue_ttm", LongType(), True),
                StructField("gross_profit_ttm", LongType(), True),
                StructField("diluted_eps_ttm", FloatType(), True),
                StructField("quarterly_earnings_growth_yoy", FloatType(), True),
                StructField("quarterly_revenue_growth_yoy", FloatType(), True),
                StructField("analyst_target_price", FloatType(), True),
                StructField("analyst_rating_strong_buy", ByteType(), True),
                StructField("analyst_rating_buy", ByteType(), True),
                StructField("analyst_rating_hold", ByteType(), True),
                StructField("analyst_rating_sell", ByteType(), True),
                StructField("analyst_rating_strong_sell", ByteType(), True),
                StructField("trailing_pe", FloatType(), True),
                StructField("forward_pe", FloatType(), True),
                StructField("price_to_sales_ratio_ttm", FloatType(), True),
                StructField("price_to_book_ratio", FloatType(), True),
                StructField("ev_to_revenue", FloatType(), True),
                StructField("ev_to_ebitda", FloatType(), True),
                StructField("beta", FloatType(), True),
                StructField("52_week_high", FloatType(), True),
                StructField("52_week_low", FloatType(), True),
                StructField("50_day_moving_average", FloatType(), True),
                StructField("200_day_moving_average", FloatType(), True),
                StructField("shares_outstanding", LongType(), True),
                StructField("shares_float", LongType(), True),
                StructField("percent_insiders", FloatType(), True),
                StructField("percent_institutions", FloatType(), True),
                StructField("dividend_date", DateType(), True),
                StructField("ex_dividend_date", DateType(), True),
            ]
        )

    def find_original_name(self, name):
        names = {
                "symbol" : "Symbol",
                "asset_type" : "AssetType",
                "name" : "Name",
                "description" : "Description",
                "cik" : "CIK",
                "exchange" : "Exchange",
                "currency" : "Currency",
                "country" : "Country",
                "sector" : "Sector",
                "industry" : "Industry",
                "address" : "Address",
                "official_site" : "OfficialSite",
                "fiscal_year_end" : "FiscalYearEnd",
                "latest_quarter" : "LatestQuarter",
                "market_capitalization" : "MarketCapitalization",
                "ebitda" : "EBITDA",
                "pe_ratio" : "PERatio",
                "peg_ratio" : "PEGRatio",
                "book_value" : "BookValue",
                "dividend_per_share" : "DividendPerShare",
                "dividend_yield" : "DividendYield",
                "eps" : "EPS",
                "revenue_per_share_ttm" : "RevenuePerShareTTM",
                "profit_margin" : "ProfitMargin",
                "operating_margin_ttm" : "OperatingMarginTTM",
                "return_on_assets_ttm" : "ReturnOnAssetsTTM",
                "return_on_equity_ttm" : "ReturnOnEquityTTM",
                "revenue_ttm" : "RevenueTTM",
                "gross_profit_ttm" : "GrossProfitTTM",
                "diluted_eps_ttm" : "DilutedEPSTTM",
                "quarterly_earnings_growth_yoy" : "QuarterlyEarningsGrowthYOY",
                "quarterly_revenue_growth_yoy" : "QuarterlyRevenueGrowthYOY",
                "analyst_target_price" : "AnalystTargetPrice",
                "analyst_rating_strong_buy" : "AnalystRatingStrongBuy",
                "analyst_rating_buy" : "AnalystRatingBuy",
                "analyst_rating_hold" : "AnalystRatingHold",
                "analyst_rating_sell" : "AnalystRatingSell",
                "analyst_rating_strong_sell" : "AnalystRatingStrongSell",
                "trailing_pe" : "TrailingPE",
                "forward_pe" : "ForwardPE",
                "price_to_sales_ratio_ttm" : "PriceToSalesRatioTTM",
                "price_to_book_ratio" : "PriceToBookRatio",
                "ev_to_revenue" : "EVToRevenue",
                "ev_to_ebitda" : "EVToEBITDA",
                "beta" : "Beta",
                "52_week_high" : "52WeekHigh",
                "52_week_low" : "52WeekLow",
                "50_day_moving_average" : "50DayMovingAverage",
                "200_day_moving_average" : "200DayMovingAverage",
                "shares_outstanding" : "SharesOutstanding",
                "shares_float" : "SharesFloat",
                "percent_insiders" : "PercentInsiders",
                "percent_institutions" : "PercentInstitutions",
                "dividend_date" : "DividendDate",
                "ex_dividend_date" : "ExDividendDate",
                "filename" : "filename",
                "_corrupt" : "_corrupt"
        }
        
        original_name = names.get(name)
        if original_name is None:
            print(f"Couldn't find name '{name}'")
        return original_name

    def apply(self, raw_df : DataFrame) -> DataFrame:
        df = raw_df

        struct_type = self.build_struct_type()

        df = df.select([
            col(self.find_original_name(f.name)).cast(f.dataType).alias(f.name) 
            if self.find_original_name(f.name) in df.columns 
            else lit(None).cast(f.dataType).alias(f.name)
            for f in struct_type
        ])

        return df