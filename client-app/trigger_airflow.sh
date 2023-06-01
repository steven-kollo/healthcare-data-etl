#!/bin/bash
str="[$(gcloud compute instances describe --zone=us-central1-a healthcare-etl-instance --format='value[](metadata.items)')]"
json=`echo "$str" | sed $'s/\'/"/g' | sed $'s/\;/,/g'`
for row in $(echo "${json}" | jq -r '.[] | @base64'); do
    _jq() {
     echo ${row} | base64 --decode | jq -r ${2}
    }
   echo $(_jq '.key')
done

# | python3 -c "import sys, json; print(json.load(sys.stdin)['name'])"
# echo "$arr"
# for obj in $arr; do
#     echo $obj 
#     echo "-"
# done
# while true
# do 
#     str1="$(gcloud compute instances describe --zone=us-central1-a healthcare-etl-instance \
#         --format='value[](metadata.items.test)')"
#     str2="sonya"
    
#     if [ "$str1" == "$str2" ]; then
#         echo "Changed"
#         gcloud composer environments run healthcare --location us-central1 dags trigger -- read_bucket_file
#         gcloud compute instances add-metadata healthcare-etl-instance --zone=us-central1-a --metadata=test=test
#     else
#         echo "$str1"
#         sleep 1
#     fi   
# done

