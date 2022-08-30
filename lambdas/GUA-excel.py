import pandas as pd
import boto3
import urllib

def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    print('Before Try --->  Source Bucket Name: (Name of the source Bucket)' + source_bucket)
    object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    print('Before Try --->  Object key (The name of the file Upload): ' + object_key)
    file_name = str(object_key.split('/')[-1])
    print('Before Try ----> File name: (Single File Name) ' + file_name)
    copy_source = {'Bucket': source_bucket, 'Key': object_key}
    
    try:
        df = pd.read_excel(object_key, sheet_name=None)  
        for key in df.keys(): 
            df[key].to_csv('{}--{}.csv'.format(file_name,key))
            # copy the file to the csv-main folder and delete both the original file and the csv file from excel folder
            s3.meta.client.copy(copy_source, source_bucket, 'csv-main/' + key)
            s3.meta.client.delete_object(Bucket=source_bucket,Key=object_key)
            
    except Exception as err:
        print ("Error -"+str(err))