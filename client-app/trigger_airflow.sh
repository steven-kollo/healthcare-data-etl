#!/bin/bash
echo "Triggering Airflow started"
sudo gcloud composer environments run healthcare --location us-central1 dags trigger -- read_bucket_file
echo "Triggering Airflow finished"