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

    csv_file_name = file_name.split('.')[0]
    print('CSV File Name: ' + csv_file_name)

    try:
        # take the excel file inside this tmp folder.
        # break it up into seperate csv files.
        # load those CSV files into csv-main folder.
        # delete the excel file from the tmp folder.
        # delete the csv files from the tmp folder.

        file_path = 's3://' + source_bucket + '/' + object_key
        print('File Path: ' + file_path)
        df = pd.read_excel(file_path, sheet_name=None)
        for key in df.keys():
            df[key].to_csv('tmp/{}--{}.csv'.format(csv_file_name, key))
            # copy the file to the csv-main folder and delete both the original file and the csv file from excel folder
            s3.meta.client.copy(copy_source, source_bucket, 'csv-main/' + key)
            s3.meta.client.delete_object(Bucket=source_bucket, Key=object_key)


        # pandas file path
        # full file path to the file in S3
        df = pd.read_excel(file_path, sheet_name=None)
        print('df: ' + str(df))
        for key in df.keys():
            df[key].to_csv('{}--{}.csv'.format(csv_file_name, key))
            print('df[key]: ' + str(df[key]))
            # copy the file to the csv-main folder and delete both the original file and the csv file from excel folder
            s3.meta.client.copy(copy_source, source_bucket, 'csv-main/' + key)

        s3.Object(source_bucket, object_key).delete()

    except Exception as err:
        s3.Object(source_bucket, object_key).delete()
        print("Error -"+str(err))
