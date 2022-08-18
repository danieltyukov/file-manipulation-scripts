import boto3
import re

def lambda_handler(event, context):

    s3 = boto3.resource('s3')

    source_bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    file_type = object_key.split('.')[-1]
    file_name = object_key.split('/')[-1]

    file_name = re.sub("[!@#$%^&*()[]{};:,./<>?\|`~-=_+]", "", file_name)

    print('Source bucket: ' + source_bucket)
    print('Object key: ' + object_key)
    print('File type: ' + file_type)
    print('File name: ' + file_name)
    
    try:

        # if file xlsx copy file to excel folder, if file csv copy file to csv-main folder, if file hyper copy file to hyper folder
        copy_source = {'Bucket': source_bucket, 'Key': object_key}
        if file_type == 'xlsx':
            s3.meta.client.copy(copy_source, source_bucket, 'excel/' + file_name)
        elif file_type == 'csv':
            s3.meta.client.copy(copy_source, source_bucket, 'csv-main/' + file_name)
        elif file_type == 'hyper':
            s3.meta.client.copy(copy_source, source_bucket, 'hyper/' + file_name)
        else:
            s3.meta.client.copy(copy_source, source_bucket, 'reject/' + file_name)

        # copy file to archive folder
        s3.meta.client.copy(copy_source, source_bucket, 'archive/' + file_name)

        # delete file from source bucket
        s3.meta.client.delete_object(Bucket=source_bucket, Key=object_key)
        
    except Exception as err:
        print ("Error -"+str(err))