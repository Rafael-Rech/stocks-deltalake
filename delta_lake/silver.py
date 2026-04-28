from spark_helper import get_spark

from transformations.silver.ffr_silver_transformation import FfrSilverTransformation
from transformations.silver.inflation_silver_transformation import InflationSilverTransformation
from transformations.silver.sma_silver_transformation import SmaSilverTransformation
from transformations.silver.tsd_silver_transformation import TsdSilverTransformation
from transformations.silver.silver_transformation import SilverTransformation
from transformations.silver.company_overview_silver_transformation import CompanyOverviewSilverTransformation

from setup import create_silver_processing_table

from typing import List
import gc
import time

start_time = time.time()

# delete_previous_data = True
delete_previous_data = False

create_silver_processing_table(delete_previous_data=delete_previous_data)

transformations : List[SilverTransformation] = [
    FfrSilverTransformation(),
    InflationSilverTransformation(),
    SmaSilverTransformation(),
    TsdSilverTransformation(),
    CompanyOverviewSilverTransformation(),
]

for index, transformation in enumerate(transformations):
    transformation.transform(start_time=start_time, delete_previous_data=delete_previous_data)

del transformations
gc.collect()