#!/bin/bash
echo "Triggering Airflow started"
gcloud composer environments run healthcare --location us-central1 dags trigger -- read_bucket_file
sleep 30
echo "Triggering Airflow finished"