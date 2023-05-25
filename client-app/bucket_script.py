
from google.cloud import storage


def test():
    storage_client = storage.Client()
    print(storage_client)
    bucket_name = "healthcare-data-bucket"
    bucket = storage_client.bucket(bucket_name)
    print(bucket)


test()


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    # """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "healthcare-data-bucket"
    # The path to your file to upload
    source_file_name = "local/path/to/file"
    # The ID of your GCS object
    destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    return
    blob = bucket.blob(destination_blob_name)

    generation_match_precondition = 0

    blob.upload_from_filename(
        source_file_name, if_generation_match=generation_match_precondition)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )
