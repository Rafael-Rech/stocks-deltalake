from airflow.sdk import task, dag
from airflow.providers.standard.operators.empty import EmptyOperator
import pendulum
import json
from io import BytesIO

from services.stocks_service import StocksService
from services.storage_service import StorageService

@dag(
    dag_id = "dag_federal_funds_rate",
    description = "Loads USA's funds rate data from an API and saves into a MinIO bucket",
    schedule = "2 2 2 * *",
    start_date = pendulum.datetime(2026, 4, 1, tz = "America/Sao_Paulo"),
    catchup = True
)
def dag_federal_funds_rate():
    stocks_service = StocksService()
    storage_service = StorageService()

    start = EmptyOperator(task_id = "start", pool = "api_rate_limit")

    @task(task_id = "manipulate_data", pool="api_rate_limit")
    def manipulate_data():
        content : dict = stocks_service.get_federal_funds_rate(read_from_file = False)
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

        first_date = first_date[:7].replace("-", "_")
        last_date = last_date[:7].replace("-", "_")

        data_bytes = json.dumps(content).encode("utf-8")

        # object_name=f"landing-zone/process/federal_funds_rate/ffr_{last_date}-{first_date}.json"
        object_name=f"landing-zone/federal_funds_rate/ffr_{last_date}-{first_date}.json"

        storage_service.put_object(
            object_name=object_name,
            data = BytesIO(data_bytes),
            length = len(data_bytes)
        )
        return 0

    end = EmptyOperator(task_id = "end")

    start >> manipulate_data() >> end

dag_federal_funds_rate()