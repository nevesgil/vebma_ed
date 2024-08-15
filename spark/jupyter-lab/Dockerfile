FROM bitnami/spark:latest

RUN pip install --no-cache-dir \
    black==24.2.0 \
    pandas==2.2.0 \
    numpy==1.26.4 \
    Faker \
    boto3 \
    minio \
    jupyterlab

WORKDIR /app

EXPOSE 8888