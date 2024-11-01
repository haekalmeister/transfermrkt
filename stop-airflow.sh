#!/bin/bash

if [ -f airflow/airflow-webserver.pid ]; then
    kill $(cat airflow/airflow-webserver.pid)
    rm airflow/airflow-webserver.pid
fi

if [ -f airflow/scheduler/airflow-scheduler.pid ]; then
    kill $(cat airflow/scheduler/airflow-scheduler.pid)
    rm airflow/scheduler/airflow-scheduler.pid
fi

echo "Airflow processes stopped"
