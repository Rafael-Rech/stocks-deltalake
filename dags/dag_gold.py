from airflow.sdk import dag
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

import datetime

@dag(
    dag_id = "dag_gold"
)
def dag_gold():
    start = EmptyOperator(task_id = "start")

    latest = LatestOnlyOperator(task_id = "latest")

    external_task = ExternalTaskSensor(
        task_id = "wait_for_silver",
        external_dag_id="dag_silver",
        external_task_id="end",
        execution_delta = datetime.timedelta(hours=1),
        poke_interval=60, #seconds
        mode="reschedule"
    )

    execute = BashOperator(task_id = "execute",
                           bash_command="/opt/airflow/project_stocks/scripts/run_spark.sh gold.py",
                           )

    end = EmptyOperator(task_id = "end")

    start >> latest >> external_task >> execute >> end

dag_gold()