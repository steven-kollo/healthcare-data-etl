
from google.cloud import storage


def upload_blob(file_name, file):
    bucket_name = 'healthcare-data-bucket'
    # source_file_name = f"uploads/{file_name}"
    contents = file
    destination_blob_name = file_name

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(contents)

    print(
        f"File {file_name} uploaded to {destination_blob_name}."
    )
