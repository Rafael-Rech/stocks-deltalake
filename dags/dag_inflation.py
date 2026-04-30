from airflow.sdk import task, dag
from airflow.providers.standard.operators.empty import EmptyOperator
import pendulum
import json
from io import BytesIO

from services.stocks_service import StocksService
from services.storage_service import StorageService

@dag(
    dag_id = "dag_inflation",
    description = "Loads inflation data from an API and saves into a MinIO bucket",
    # schedule = "@yearly",
    schedule = "2 2 2 1 *",
    start_date = pendulum.datetime(2025, 12, 30, tz = "America/Sao_Paulo"),
    catchup = True
)
def dag_inflation():
    stocks_service = StocksService()
    storage_service = StorageService()

    start = EmptyOperator(task_id = "start")

    @task(task_id = "manipulate_data", pool = "api_rate_limit")
    def manipulate_data():
        content : dict = stocks_service.get_inflation(read_from_file = False)
        if content is None or "data" not in content.keys():
            raise ValueError("Invalid content")
        
        data = content.get("data")
        
        if data is None or not(isinstance(data, list)) or len(data) == 0:
            raise ValueError("No data found in content")
        
        first : dict = data[0]
        last : dict = data[-1]

        first_date = first.get("date", None)
        last_date = last.get("date", None)

        if first_date is None or last_date is None:
            raise ValueError("No date information found in the data")

        first_year = first_date[:4]
        last_year = last_date[:4]

        data_bytes = json.dumps(content).encode("utf-8")

        # object_name = f"landing-zone/process/inflation/inflation_{last_year}-{first_year}.json"
        object_name = f"landing-zone/inflation/inflation_{last_year}-{first_year}.json"

        storage_service.put_object(
            object_name=object_name,
            data = BytesIO(data_bytes),
            length = len(data_bytes)
        )
        return 0

    end = EmptyOperator(task_id = "end")

    start >> manipulate_data.override(pool = "api_rate_limit")() >> end

dag_inflation()