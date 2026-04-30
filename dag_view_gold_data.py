from airflow.sdk import task, dag
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

import pendulum
import datetime

@dag(
    dag_id = "dag_view_gold_data",
    catchup = False
)
def dag_view_gold_data():
    start = EmptyOperator(task_id = "start")

    execute = BashOperator(task_id = "execute", 
                           bash_command="/opt/airflow/project_stocks/scripts/run_spark.sh view_gold_data.py",
                          )

    end = EmptyOperator(task_id = "end")

    start >> execute >> end

dag_view_gold_data()