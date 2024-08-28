FROM bitnami/spark:latest

RUN pip install --no-cache-dir \
    black==24.2.0 \
    pandas==2.2.0 \
    numpy==1.26.4 \
    Faker \
    boto3 \
    minio


COPY ./jars/hadoop-aws-3.3.4.jar /opt/bitnami/spark/jars/
COPY ./jars/aws-java-sdk-bundle-1.12.262.jar /opt/bitnami/spark/jars/
COPY ./jars/delta-core_2.12-2.1.0.jar /opt/bitnami/spark/jars/