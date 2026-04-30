from airflow.sdk import task, dag
from airflow.providers.standard.operators.empty import EmptyOperator
import pendulum
import json
from io import BytesIO
from time import sleep

from services.stocks_service import StocksService
from services.storage_service import StorageService

@dag(
    dag_id = "dag_simple_moving_average",
    description = "Loads a company's SMA data from an API and saves into a MinIO bucket",
    schedule = "0 7 * * 1",
    start_date = pendulum.datetime(2026, 4, 11, tz = "America/Sao_Paulo"),
    catchup = True,
)
def dag_simple_moving_average():
    stocks_service = StocksService()
    storage_service = StorageService()

    start = EmptyOperator(task_id = "start")

    @task(task_id = "manipulate_data", pool = "api_rate_limit")
    def manipulate_data(symbol : str = ""):
        sleep(2.5)
        content : dict = stocks_service.get_simple_moving_average(symbol = symbol, time_period = 100, interval = "weekly", read_from_file = False)
        if content is None or "Technical Analysis: SMA" not in content.keys():
            raise ValueError("Invalid content")
        
        analysis = content.get("Technical Analysis: SMA")
        
        if analysis is None or not(isinstance(analysis, dict)) or len(analysis) == 0:
            raise ValueError("No data found in content")

        keys = list(analysis.keys())

        keys.sort()

        if len(keys) == 0:
            raise ValueError("No date information found in the data")
             
        first_date = keys[0]
        last_date = keys[-1]

        first_date = first_date.replace("-", "_")
        last_date = last_date.replace("-", "_")

        data_bytes = json.dumps(content).encode("utf-8")

        # object_name=f"landing-zone/process/simple_moving_average/sma_{symbol}_{first_date}-{last_date}.json"
        object_name=f"landing-zone/simple_moving_average/sma_{symbol}_{first_date}-{last_date}.json"

        storage_service.put_object(
            object_name=object_name,
            data = BytesIO(data_bytes),
            length = len(data_bytes)
        )
        return 0

    end = EmptyOperator(task_id = "end")

    symbols = ["IBM",
               "DIS",
               "MSFT",
               "NVDA",
               ]

    
    start >> manipulate_data.override(pool = "api_rate_limit").expand(symbol = symbols) >> end

dag_simple_moving_average()