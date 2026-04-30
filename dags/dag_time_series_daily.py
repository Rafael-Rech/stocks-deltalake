from airflow.sdk import task, dag
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
import pendulum
from time import sleep
import json
from io import BytesIO

from services.stocks_service import StocksService
from services.storage_service import StorageService

@dag(
    dag_id = "dag_time_series_daily",
    description = "Loads a company's time series for the day from an API and saves into a MinIO bucket",
    schedule = "0 8 * * *",
    start_date = pendulum.datetime(2026, 4, 15, tz = "America/Sao_Paulo"),
    catchup = True
)
def dag_time_series_daily():
    stocks_service = StocksService()
    storage_service = StorageService()

    start = EmptyOperator(task_id = "start")

    latest = LatestOnlyOperator(task_id = "latest")

    @task(task_id = "manipulate_data", pool = "api_rate_limit")
    def manipulate_data(symbol : str = ""):
        sleep(3.0)

        content : dict = stocks_service.get_time_series_daily(symbol=symbol, read_from_file=False)

        if content is None or "Time Series (Daily)" not in content:
            raise ValueError("Invalid content")
        
        series = content.get("Time Series (Daily)")

        if series is None or not(isinstance(series, dict)) or len(series) == 0:
            raise ValueError("No data found in content")

        keys = list(series.keys())

        keys.sort()

        if len(keys) == 0:
            raise ValueError("No date information found in the data")
        
        first_date = keys[0].replace("-", "_")
        last_date = keys[-1].replace("-", "_")

        data_bytes = json.dumps(content).encode("utf-8")

        # object_name=f"landing-zone/process/time_series_daily/tsd_{symbol}_{first_date}-{last_date}.json"
        object_name=f"landing-zone/time_series_daily/tsd_{symbol}_{first_date}-{last_date}.json"

        storage_service.put_object(
            object_name=object_name,
            data=BytesIO(data_bytes),
            length=len(data_bytes)
        )
        
        return 0


    end = EmptyOperator(task_id = "end")

    symbols = ["IBM",
               "DIS",
               "MSFT",
               "NVDA",
              ]

    start >> latest >> manipulate_data.override(pool = "api_rate_limit").expand(symbol = symbols) >> end    



dag_time_series_daily()