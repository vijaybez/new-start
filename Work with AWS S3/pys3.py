# python39 python

'''
This is a script that interacts with AWS S3 using boto3 package. The code is designed to perform basic S3 functions such are creating and deleting buckets and files in S3.
We can also set certain security options such as blocking public access on buckets.
it also uses the try except defination for every method to capture any exceptions and print them on teh screen. The actual production version of this code would log the error to a log file

This was run in a virtual environment using PycHarm and setting up environmental variables for ACcess and Secret Key.
Here are the list of methods:

create_bucket
block_public_access
list_objects
copy_file
generate_presigned_url
upload_files
download_files
delete_files
delete_bucket
'''

import os
import boto3
from botocore.exceptions import ClientError

AWS_ACCESS_KEY = 'ACCESS_KEY'
AWS_SECRET_KEY = 'SECRET_KEY'
SYNC_FOLDER = 'C:/Users/sharanya/s3_objects'
DOWNLOAD_FOLDER = 'C:/Users/sharanya/s3_download'
BUCKET_NAME = 'vijayaws202001013'
OTHER_BUCKET = 'vijayaws202001014'
F1 = 'titanic.csv'
F2 = 'heart.csv'


def upload_files(bucket_name, directory, local_file, s3_connection, s3path=None):
    file_path = directory + '/' + local_file
    remote_path = s3path
    if remote_path is None:
        remote_path = local_file
    try:
        s3_connection.Bucket(bucket_name).upload_file(file_path, remote_path)
    except ClientError as ce:
        print('error', ce)


def download_files(bucket_name, key_file, directory, local_file, s3_connection):
    local_file_path = directory + '/' + local_file

    try:
        s3_connection.Bucket(bucket_name).download_file(key_file, local_file_path)
    except ClientError as ce:
        print('error', ce)


def delete_files(bucket_name, key_files, s3_connection):
    objects = []

    for key in key_files:
        objects.append({'Key': key})
    try:
        s3_connection.Bucket(bucket_name).delete_objects(Delete={'Objects': objects})
    except ClientError as ce:
        print('error', ce)


def create_bucket(bucket_name, s3_connection, secure=False):
    try:
        s3_connection.create_bucket(Bucket=bucket_name)
        if secure:
            block_public_access(bucket_name, s3_connection)
    except ClientError as ce:
        print('error', ce)


def list_objects(bucket_name, s3_connection):
    try:
        response = s3_connection.meta.client.list_objects_v2(Bucket=bucket_name)
        objects = []
        for content in response['Contents']:
            objects.append(content['Key'])
        print('Number of Objects in bucket ', bucket_name, 'is ', len(objects))
        return objects
    except ClientError as ce:
        print('error', ce)


def copy_file(source_bucket, source_key, dest_bucket, dest_key, s3_connection):
    try:
        source_dict = {'Key': source_key, 'Bucket': source_bucket}
        s3_connection.Bucket(dest_bucket).copy(source_dict, dest_key)
    except ClientError as ce:
        print('error', ce)


def block_public_access(bucket_name, s3_connection):
    try:
        s3_connection.meta.client.put_public_access_block(Bucket=bucket_name,
                                                          PublicAccessBlockConfiguration={
                                                              'BlockPublicAcls': True,
                                                              'IgnorePublicAcls': True,
                                                              'BlockPublicPolicy': True,
                                                              'RestrictPublicBuckets': True

                                                          })
    except ClientError as ce:
        print('error', ce)


def generate_presigned_url(bucket_name, key_file, expiry_secs, s3_connection):
    try:
        response = s3_connection.meta.client.generate_presigned_url('get_object', Params={
            'Bucket': bucket_name,
            'Key': key_file
        }, ExpiresIn=expiry_secs)
        print(response)
    except ClientError as ce:
        print('error', ce)


def delete_bucket(bucket_name,s3_connection):
    try:
        objects = list_objects(bucket_name,s3_connection)
        if len(objects)>0:
            for key in s3_connection.Bucket(bucket_name).objects.all():
                key.delete()
        s3_connection.Bucket(bucket_name).delete()
    except ClientError as ce:
        print('error',ce)

def main():
    access = os.getenv('AWS_ACCESS_KEY')
    secret = os.getenv('AWS_SECRET_KEY')
    s3_connection = boto3.resource('s3', aws_access_key_id=access, aws_secret_access_key=secret)
    delete_bucket(BUCKET_NAME,s3_connection)
    # create_bucket(BUCKET_NAME, s3_connection)
    # create_bucket(OTHER_BUCKET, s3_connection, secure=True)
    # upload_files(BUCKET_NAME, SYNC_FOLDER, F1, s3_connection)
    # upload_files(BUCKET_NAME, SYNC_FOLDER, F2, s3_connection)
    # generate_presigned_url(BUCKET_NAME, F1, 300, s3_connection)
    # download_files(BUCKET_NAME, F1, DOWNLOAD_FOLDER, F1, s3_connection)
    # delete_files(BUCKET_NAME, [F1, F2], s3_connection)
    # list_objects(BUCKET_NAME,s3_connection)
    # copy_file(BUCKET_NAME,F1,OTHER_BUCKET,F1,s3_connection)


if __name__ == '__main__':
    main()
