import boto3
from botocore.client import Config
import requests
import json
from datetime import datetime

# # MinIO configuration
minio_endpoint = "http://minio:9000"
minio_root_user = "minio"
minio_root_password = "minio123"

bucket_name = "bronze"

s3_client = boto3.client(
    "s3",
    endpoint_url=minio_endpoint,
    aws_access_key_id=minio_root_user,
    aws_secret_access_key=minio_root_password,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

try:
    s3_client.head_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' already exists.")
except Exception as e:
    s3_client.create_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' created.")


response = requests.get("https://api.openbrewerydb.org/breweries")
breweries_data = response.json()

json_data = json.dumps(breweries_data, indent=2)
json_bytes = json_data.encode("utf-8")


current_date = datetime.now()
partition_path = f"source=openbrewerydb/year={current_date.year}/month={current_date.month}/day={current_date.day}/"


object_name = (
    f"{partition_path}breweries_{current_date.strftime('%Y-%m-%d_%H-%M-%S')}.json"
)


s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=json_bytes)
print(f"JSON data uploaded to bucket '{bucket_name}' with object name '{object_name}'")

# TODO: add validation layer before sending the data into the lake