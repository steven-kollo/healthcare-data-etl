#!/bin/bash
echo "Triggering Airflow started"
touch test.txt
# gcloud composer environments run healthcare --location us-central1 dags trigger -- read_bucket_file
sleep 15
echo "Triggering Airflow finished"