from typing import List

from setup import create_gold_database, create_gold_processing_table

from transformations.gold.gold_transformation import GoldTransformation
from transformations.gold.dim_company_gold_transformation import DimCompanyGoldTransformation
from transformations.gold.dim_date_gold_transformation import DimDateGoldTransformation
from transformations.gold.dim_indicator_gold_transformation import DimIndicatorGoldTransformation
from transformations.gold.fact_macro_indicator_gold_transformation import FactMacroIndicatorGoldTransformation
from transformations.gold.fact_sma_gold_transformation import FactSmaGoldTransformation
from transformations.gold.fact_tsd_gold_transformation import FactTsdGoldTransformation

create_gold_processing_table()
create_gold_database()

#  delete_previous_data = True
delete_previous_data = False

transformations : List[GoldTransformation] = [
    DimCompanyGoldTransformation(),
    DimDateGoldTransformation(),
    DimIndicatorGoldTransformation(),
    FactMacroIndicatorGoldTransformation(),
    FactSmaGoldTransformation(),
    FactTsdGoldTransformation()
]

for transformation in transformations:
    transformation.transform(delete_previous_data=delete_previous_data)
