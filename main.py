import os
import re
import time
from datetime import datetime,timedelta
from minio import Minio
from minio.error import S3Error

from dotenv import load_dotenv


def uploadFile():
    store=os.environ["DATAPATH"]

    os.chdir(store)
    print("Current working directory: {}".format(os.getcwd()))

    dir_list = os.listdir()

    now=time.time()

    # init minio client
    client = Minio(
        host,
        access_key=access_key,
        secret_key=secret_key,
    )
    # check bucket existing
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket '{}' already exists".format(bucket))

    date = datetime.utcnow().replace(
            hour=0, minute=0, second=0, microsecond=0,
            ) + timedelta(days=30)

    for f in dir_list:
        # print(os.stat(f).st_mtime)
        if os.stat(f).st_mtime < now - 300: # go back five minutes
            print("File to upload: {}".format(f))
            print("Uploading")
            try:
                result = client.fput_object(
                    bucket, f, os.path.join(store,f),
                )
                print("Upload done: {}".format(result.object_name))
                os.remove(f)
            except Exception as e:
                print(e)

if __name__=="__main__":
    load_dotenv()

    access_key=os.environ["MINIO_ACCESS_KEY"]
    secret_key=os.environ["MINIO_SECRET_KEY"]
    host=os.environ["MINIO_URL"]
    bucket=os.environ["MINIO_BUCKET"]

    uploadFile()