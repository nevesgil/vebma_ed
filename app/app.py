from flask import Flask, jsonify, make_response
from minio import Minio
from io import BytesIO
import json

app = Flask(__name__)

# Initialize MinIO client
minio_client = Minio(
    "minio:9000", 
    access_key="minio",  
    secret_key="minio123",  
    secure=False  
)

app.config["PROPAGATE_EXCEPTIONS"] = True
app.config["API_TITLE"] = "API"
app.config["API_VERSION"] = "v1"
app.config["OPENAPI_VERSION"] = "3.0.3"
app.config["OPENAPI_URL_PREFIX"] = "/docs"
app.config["OPENAPI_SWAGGER_UI_PATH"] = "/"
app.config["OPENAPI_SWAGGER_UI_URL"] = (
"https://cdn.jsdelivr.net/npm/swagger-ui-dist/"
)

@app.route('/get-brew-json', methods=['GET'])
def get_json():
    try:
        bucket_name = "bronze"
        object_name = "breweries_2024-08-16_05-09-05.json"

        data = minio_client.get_object(bucket_name, object_name)

        json_data = json.load(BytesIO(data.read()))

        return jsonify(json_data)

    except Exception as e:
        return make_response(str(e), 500)