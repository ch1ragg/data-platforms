from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="trip_analysis",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    submit_spark_job = BashOperator(
        task_id="submit_trip_analysis",
        bash_command="""
            docker exec spark-master /opt/spark/bin/spark-submit \
                --master spark://spark-master:7077 \
                --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
                /opt/spark-jobs/process_trips.py
        """,
    )