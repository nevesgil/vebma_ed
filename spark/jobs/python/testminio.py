import boto3
from botocore.client import Config
import requests
import json
from datetime import datetime
from io import BytesIO
import pandas as pd

# MinIO configuration
minio_endpoint = "http://minio:9000"  # or the IP of your MinIO instance
minio_root_user = "minio"  # Replace with your MINIO_ROOT_USER
minio_root_password = "minio123"  # Replace with your MINIO_ROOT_PASSWORD
bucket_name = "bronze"  # Replace with your bucket name

# Initialize the S3 client
s3_client = boto3.client(
    's3',
    endpoint_url=minio_endpoint,
    aws_access_key_id=minio_root_user,
    aws_secret_access_key=minio_root_password,
    config=Config(signature_version='s3v4'),
    region_name="us-east-1"  # Region is required by boto3 but is arbitrary for MinIO
)

# Ensure the bucket exists (create it if not)
try:
    s3_client.head_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' already exists.")
except Exception as e:
    s3_client.create_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' created.")

# Fetch data from the API
response = requests.get("https://api.openbrewerydb.org/breweries")
breweries_data = response.json()

# Convert JSON data to a bytes object for upload
json_data = json.dumps(breweries_data, indent=2)
json_bytes = json_data.encode('utf-8')

# Define partitioning path based on the current date
current_date = datetime.now()
partition_path = f"source=openbrewerydb/year={current_date.year}/month={current_date.month}/day={current_date.day}/"

# Define the object name in the bucket (with timestamp to avoid overwrites)
object_name = f"{partition_path}breweries_{current_date.strftime('%Y-%m-%d_%H-%M-%S')}.json"

# Upload the JSON data to the bronze bucket in MinIO
s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=json_bytes)
print(f"JSON data uploaded to bucket '{bucket_name}' with object name '{object_name}'")

# Optional: List objects in the partitioned path to confirm the upload
objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=partition_path)
for obj in objects.get('Contents', []):
    print(f'Object: {obj["Key"]}')

# # Define the object name in the bucket (with timestamp to avoid overwrites)
# object_name = f"breweries_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"

# # Upload the JSON data to the bronze bucket in MinIO
# s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=json_bytes)
# print(f"JSON data uploaded to bucket '{bucket_name}' with object name '{object_name}'")

# # Optional: List objects in the bucket to confirm the upload
# objects = s3_client.list_objects_v2(Bucket=bucket_name)
# for obj in objects.get('Contents', []):
#     print(f'Object: {obj["Key"]}')
