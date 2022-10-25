import pandas as pd
import boto3
import urllib
import fsspec
import s3fs
import openpyxl


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

    tmp_object_key = 'tmp/' + file_name
    tmp_copy_source = {'Bucket': source_bucket, 'Key': tmp_object_key}

    csv_file_name = file_name.split('.')[0]
    print('CSV File Name: ' + csv_file_name)

    try:
        folder_path = 's3://guar-file-handler/'
        # create a tmp folder inside source_bukcet and put the object_key file inside it
        s3.meta.client.copy(copy_source, source_bucket, 'tmp/' + file_name)
        file_path = folder_path + 'tmp/' + file_name

        # file_path = 's3://' + source_bucket + '/' + object_key
        print('File Path: ' + file_path)
        df = pd.read_excel(file_path, sheet_name=None)
        for key in df.keys():
            print('Data Frame: ' + str(df))
            print('Key: ' + key)
            # df[key].to_csv('{}--{}.csv'.format(csv_file_name, key))
            df[key].to_csv('s3://guar-file-handler/csv-main/{}--{}.csv'.format(csv_file_name, key), index=False)
            print('Converted to CSV')
            print('Key: ' + key)
            # copy the file to the csv-main folder and delete both the original file and the csv file from tmp folder
            # s3.meta.client.copy(tmp_copy_source, source_bucket, 'csv-main/{}--{}.csv'.format(csv_file_name, key))
            # print('File Copied to csv-main: ', key)

        
        s3.Object(source_bucket, object_key).delete()
        s3.Object(source_bucket, tmp_object_key).delete()

        print('File has been converted to CSV and uploaded to csv-main folder')

    except Exception as err:
        s3.Object(source_bucket, object_key).delete()
        s3.Object(source_bucket, tmp_object_key).delete()
        print("Error - "+str(err))
