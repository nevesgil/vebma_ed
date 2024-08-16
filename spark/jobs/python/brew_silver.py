from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, year, input_file_name
from pyspark.sql.types import TimestampType
import boto3
from botocore.client import Config
from datetime import datetime

spark = (
    SparkSession.builder.appName("MySparkApp")
    .config(
        "spark.jars",
        "/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/bitnami/spark/jars/delta-core_2.12-2.1.0.jar",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

sc = spark.sparkContext

hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")

print("##################################")

minio_endpoint = "http://minio:9000"
minio_root_user = "minio"
minio_root_password = "minio123"
bronze_bucket = "bronze"
silver_bucket = "silver"

s3_client = boto3.client(
    "s3",
    endpoint_url=minio_endpoint,
    aws_access_key_id=minio_root_user,
    aws_secret_access_key=minio_root_password,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)


objects = s3_client.list_objects_v2(Bucket=bronze_bucket)
object_keys = [obj["Key"] for obj in objects.get("Contents", [])]

if not object_keys:
    print("No objects found in the bronze bucket.")
    spark.stop()
    exit()


latest_key = max(object_keys, key=lambda x: x.split('/')[-1])

print(f"Latest object in Bronze Bucket: {latest_key}")


df = spark.read.json(f"s3a://{bronze_bucket}/{latest_key}")


df = df.withColumn("processed_on", lit(datetime.now()).cast(TimestampType()))


df = df.withColumn("processed_by", lit("gilmarneves"))


output_path = f"s3a://{silver_bucket}/delta-table"


df.write.format("parquet").mode("overwrite").save(output_path)

print(f"Data successfully saved to {output_path} as a Delta table.")

spark.stop()