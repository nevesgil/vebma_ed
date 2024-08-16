import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="spark_flow",
    default_args={
        "owner": "Gilmar Neves",
        "start_date": airflow.utils.dates.days_ago(1),
    },
    schedule_interval="@daily",
)

start = BashOperator(
    task_id="message_start", bash_command="echo START PROCESS | figlet"
)

ingest_raw = SparkSubmitOperator(
    task_id="brew_bronze",
    conn_id="spark-conn",
    application="jobs/python/brew_bronze.py",
    dag=dag,
)

process_silver = SparkSubmitOperator(
    task_id="brew_silver",
    conn_id="spark-conn",
    application="jobs/python/brew_silver.py",
    jars="/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/bitnami/spark/jars/delta-core_2.12-2.1.0.jar",
    conf={
        "spark.hadoop.fs.s3a.access.key": "minio",
        "spark.hadoop.fs.s3a.secret.key": "minio123",
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    },
    dag=dag,
)


end = BashOperator(task_id="message_end", bash_command="echo END PROCESS | figlet")

start >> ingest_raw >> process_silver >> end
