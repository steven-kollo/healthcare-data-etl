
from google.cloud import storage


def upload_blob(file_name):
    bucket_name = 'healthcare-data-bucket'
    source_file_name = f"uploads/{file_name}"
    # The ID of your GCS object
    destination_blob_name = file_name

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    generation_match_precondition = 0

    blob.upload_from_filename(
        source_file_name, if_generation_match=generation_match_precondition)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )


upload_blob()
