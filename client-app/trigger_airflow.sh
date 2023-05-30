#!/bin/bash
while true
do 
    str1="$(gcloud compute instances describe --zone=us-central1-a healthcare-etl-instance \
        --format='value[](metadata.items.test)')"
    str2="sonya"
    
    if [ "$str1" == "$str2" ]; then
        echo "Changed"
        gcloud composer environments run healthcare --location us-central1 dags trigger -- read_bucket_file
        gcloud compute instances add-metadata healthcare-etl-instance --zone=us-central1-a --metadata=test=test
    else
        echo "Nothing changed"
        sleep 1
    fi   
done

