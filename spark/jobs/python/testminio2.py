from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, year
import boto3
from botocore.client import Config
from datetime import datetime

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .config("spark.jars", "/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar") \
    .getOrCreate()

# Access SparkContext from SparkSession
sc = spark.sparkContext

# Set Hadoop configurations for S3A
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set('fs.s3a.access.key', 'minio')
hadoop_conf.set('fs.s3a.secret.key', 'minio123')
hadoop_conf.set('fs.s3a.path.style.access', 'true')
hadoop_conf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
hadoop_conf.set('fs.s3a.endpoint', 'http://minio:9000')

print("################################## started")

# MinIO Configuration
minio_endpoint = "http://minio:9000"
minio_root_user = "minio"
minio_root_password = "minio123"
bronze_bucket = "bronze"
silver_bucket = "silver"

# Initialize the S3 client for MinIO
s3_client = boto3.client(
    's3',
    endpoint_url=minio_endpoint,
    aws_access_key_id=minio_root_user,
    aws_secret_access_key=minio_root_password,
    config=Config(signature_version='s3v4'),
    region_name="us-east-1"
)

# List all objects in the bronze bucket
objects = s3_client.list_objects_v2(Bucket=bronze_bucket)
object_keys = [obj['Key'] for obj in objects.get('Contents', [])]

if not object_keys:
    print("No objects found in the bronze bucket.")
    spark.stop()
    exit()

print(f"Objects in Bronze Bucket: {object_keys}")

# Copy each object from the bronze bucket to the silver bucket
for key in object_keys:
    copy_source = {'Bucket': bronze_bucket, 'Key': key}
    s3_client.copy_object(CopySource=copy_source, Bucket=silver_bucket, Key=key)
    print(f"Copied {key} from {bronze_bucket} to {silver_bucket}")

print("All objects copied to the silver bucket.")

# Read all JSON data from Silver bucket into a PySpark DataFrame
df = spark.read.json(f"s3a://{silver_bucket}/")

# Add a new column "createdby" with the default value "gilmarneves"
df = df.withColumn("createdby", lit("gilmarneves"))

# Add a year column based on the creation date
df = df.withColumn("year", year(lit(datetime.now())))

# Save the DataFrame to the Silver bucket in Parquet format, partitioned by year
output_path = f"s3a://{silver_bucket}/"
print(f"Saving DataFrame to path: {output_path}")
df.write.mode("overwrite").partitionBy("year").parquet(output_path)

print("Data successfully saved to the Silver bucket as Parquet, partitioned by year.")

# Stop the Spark session
spark.stop()
