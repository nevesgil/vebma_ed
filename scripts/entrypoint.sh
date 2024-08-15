#!/usr/bin/env bash

# Initialize the Airflow database
airflow db init

airflow db upgrade

# Create an admin user
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Start the Airflow webserver
airflow webserver

