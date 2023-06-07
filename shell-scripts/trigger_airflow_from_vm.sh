#!/bin/bash
while sleep 1; do
    str="[$(gcloud compute instances describe --zone=us-central1-a healthcare-etl-instance --format='value[](metadata.items)')]"
    json=`echo "$str" | sed $'s/\'/"/g' | sed $'s/\;/,/g'`
    for row in $(echo "${json}" | jq -r '.[] | @base64'); do
        _jq() {
        echo ${row} | base64 --decode | jq -r ${1}
        }
        status=`echo $(_jq '.value')`
        target="f"
        if [ "$status" == "$target" ]; then
            filename=`echo $(_jq '.key')`
            echo "New file uploaded!"
            echo $filename
            echo "Processing file..."
            json=$"{\"filename\":\"$filename\"}"
            gcloud compute instances add-metadata healthcare-etl-instance --zone=us-central1-a --metadata="$filename"=t
            gcloud composer environments run healthcare --location us-central1 dags trigger -- read_bucket_file --conf "$json"
            break;
        fi;
    done;
    echo "Run end"
done;