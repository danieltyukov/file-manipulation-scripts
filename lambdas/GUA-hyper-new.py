import boto3
import urllib
import pantab as pd

def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    print('Before Try --->  Source Bucket Name: (Name of the source Bucket)' + source_bucket)
    object_key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'])
    print('Before Try --->  Object key (The name of the file Upload): ' + object_key)
    file_name = str(object_key.split('/')[-1])
    print('Before Try ----> File name: (Single File Name) ' + file_name)
    copy_source = {'Bucket': source_bucket, 'Key': object_key}

    csv_file_name = file_name.split('.')[0]
    print('CSV File Name: ' + csv_file_name)

    try:
        print('working on it')

    except Exception as err:
        s3.Object(source_bucket, object_key).delete()
        print("Error - "+str(err))

    
