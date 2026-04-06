from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import docker

def submit_spark_job():
    client = docker.DockerClient(base_url="unix:///var/run/docker.sock")
    container = client.containers.get("spark-master")
    
    exit_code, output = container.exec_run(
        cmd="/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "/opt/spark-jobs/process_trips.py",
        user="root",
    )
    
    print(output.decode("utf-8"))
    
    if exit_code != 0:
        raise Exception(f"Spark job failed with exit code {exit_code}")

with DAG(
    dag_id="trip_analysis",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    spark_task = PythonOperator(
        task_id="submit_trip_analysis",
        python_callable=submit_spark_job,
    )