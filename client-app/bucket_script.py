
from google.cloud import storage


def upload_blob(file_name, file):
    bucket_name = 'healthcare-data-bucket'
    contents = file  # string file here

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    blob.upload_from_string(contents)

    print(
        f"File {file_name} uploaded to {bucket_name}."
    )
    return True
