from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

with DAG(
    dag_id="trip_analysis",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    submit_spark_job=DockerOperator(
        task_id="submit_trip_analysis",
        image="apache/spark:3.5.0",
        command = 
        """
         /opt/spark/bin/spark-submit \
            -- master spark://spark-master:7077 \
            -- packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-jobs/process_trips.py
        """,
        network_mode="data-platforms-default",
        mounts=[
            "c/Users/everb/OneDrive/Desktop/data-platforms/spark-jobs"
        ],
        docker_url="unix:///var/run/docker.sock",
        auto_remove=True,
    )