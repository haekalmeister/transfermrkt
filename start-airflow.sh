#!/bin/bash
# Start Airflow scheduler in the background
airflow scheduler &
# Start Airflow webserver in the background
airflow webserver -p 8080 &
