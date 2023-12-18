import os
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'formal-grammar-407607-97b29afb27d0.json'
storage_client = storage.Client()

#create bucket
def create_bucket(bucket_name):
    bucket = storage_client.bucket(bucket_name)
    bucket = storage_client.create_bucket(bucket)


my_bucket = storage_client.get_bucket('igor_data_bucket')


#to upload
def upload_to_bucket(blob_name, file_path, bucket_name):
    try:
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(file_path)
        return True
    except Exception as e:
        print(e)
        return False

