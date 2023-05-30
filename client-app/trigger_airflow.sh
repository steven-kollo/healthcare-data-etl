#!/bin/bash
echo "Triggering Airflow started"
touch test.txt
# gcloud composer environments run healthcare --location us-central1 dags trigger -- read_bucket_file
echo "Triggering Airflow finished"

curl "http://metadata.google.internal/computeMetadata/v1" -H "Metadata-Flavor: Google"


# ADD
gcloud compute instances add-metadata healthcare-etl-instance --zone=us-central1-a --metadata=test=1488


gcloud compute instances describe healthcare-etl-instance \
  --format='value[](metadata.items.test)'