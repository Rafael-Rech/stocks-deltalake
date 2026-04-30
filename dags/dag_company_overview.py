from airflow.sdk import task, dag
from airflow.providers.standard.operators.empty import EmptyOperator
import pendulum

from services.stocks_service import StocksService
from services.storage_service import StorageService

from datetime import datetime
import json
from io import BytesIO
from time import sleep

@dag(
    dag_id = "dag_company_overview",
    description = "Loads data from companies",
    schedule = "0 15 5 * *",
    start_date = pendulum.datetime(2026, 4, 4, tz = "America/Sao_Paulo"),
    catchup = False
)
def dag_company_overview():
    stocks_service = StocksService()
    storage_service = StorageService()

    start = EmptyOperator(task_id = "start")

    
    @task(task_id = "manipulate_data", pool = "api_rate_limit")
    def manipulate_data(symbol : str):
        sleep(2.5)
        content : dict = stocks_service.get_company_overview(symbol)

        if content is None or len(content.keys()) == 0:
            raise ValueError("Invalid content")
        
        now = datetime.now()
        year = now.year
        month = now.month
        day = now.day
        object_name=f"landing-zone/company_overview/co_{symbol}_{year}_{month}_{day}.json"

        data_bytes = json.dumps(content).encode("utf-8")

        storage_service.put_object(
            object_name=object_name,
            data=BytesIO(data_bytes),
            length=len(data_bytes)
        )

        return 0

    end = EmptyOperator(task_id = "end")

    symbols = [
               "IBM",
               "DIS",
               "MSFT",
               "NVDA",
               ]

    start >> manipulate_data.override(pool = "api_rate_limit").expand(symbol = symbols) >> end

dag_company_overview()