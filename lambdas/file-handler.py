import boto3
import re
import time
import urllib

def lambda_handler(event, context):

    s3 = boto3.resource('s3')
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    print('Before Try --->  Source Bucket Name: (Name of the source Bucket)' + source_bucket)
    object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    print('Before Try --->  Object key (The name of the file Upload): ' + object_key)
    file_type = object_key.split('.')[-1]
    print('Before Try --->  File type: (CSV XLSX HYPER)' + file_type)
    file_name = str(object_key.split('/')[-1])
    print('Before Try ----> File name: (Single File Name) ' + file_name)
    copy_source = {'Bucket': source_bucket, 'Key': object_key}
    try:
    
        if file_type == 'csv':  
            try:
                s3.meta.client.copy(copy_source, source_bucket, 'csv-main/' + file_name)
                s3.meta.client.copy(copy_source, source_bucket, 'archive/' + file_name)
                print("The file was copied to csv-main")
            except Exception as e:
                s3.meta.client.copy(copy_source, source_bucket, 'unprocessed/' + file_name)
                print("The file was not copied")
            try:                        
                s3.meta.client.delete_object(Bucket=source_bucket,Key=object_key)
                print("The file was deleted")
            except Exception as e:
                print("The file was not deleted")
        elif file_type == 'xlsx':
            try:
                s3.meta.client.copy(copy_source, source_bucket, 'excel/' + file_name)
                s3.meta.client.copy(copy_source, source_bucket, 'archive/' + file_name)
                print("The file was copied to excel")
            except Exception as e:
                s3.meta.client.copy(copy_source, source_bucket, 'unprocessed/' + file_name)
                print("The file was not copied")
            try:                        
                s3.meta.client.delete_object(Bucket=source_bucket,Key=object_key)
                print("The file was deleted")
            except Exception as e:
                print("The file was not deleted")
        elif file_type == 'hyper':
            try:
                s3.meta.client.copy(copy_source, source_bucket, 'hyper/' + file_name)
                s3.meta.client.copy(copy_source, source_bucket, 'archive/' + file_name)
                print("The file was copied to hyper")
            except Exception as e:
                s3.meta.client.copy(copy_source, source_bucket, 'unprocessed/' + file_name)
                print("The file was not copied")
            try:                        
                s3.meta.client.delete_object(Bucket=source_bucket,Key=object_key)
                print("The file was deleted")
            except Exception as e:
                print("The file was not deleted")
        else:
            s3.meta.client.copy(copy_source, source_bucket, 'reject/' + file_name)
            s3.meta.client.copy(copy_source, source_bucket, 'archive/' + file_name)
            try:                        
                s3.meta.client.delete_object(Bucket=source_bucket,Key=object_key)
                print("The file was deleted")
            except Exception as e:
                print("The file was not deleted")
            
    except Exception as err:
        print ("Error -"+str(err))