# import pandas as pd
# import boto3
# import urllib

# def lambda_handler(event, context):
#     s3 = boto3.resource('s3')
#     source_bucket = event['Records'][0]['s3']['bucket']['name']
#     print('Before Try --->  Source Bucket Name: (Name of the source Bucket)' + source_bucket)
#     object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
#     print('Before Try --->  Object key (The name of the file Upload): ' + object_key)
#     file_name = str(object_key.split('/')[-1])
#     print('Before Try ----> File name: (Single File Name) ' + file_name)
#     copy_source = {'Bucket': source_bucket, 'Key': object_key}

#     try:
#         df = pd.read_excel(object_key, sheet_name=None)
#         for key in df.keys():
#             df[key].to_csv('{}--{}.csv'.format(file_name,key))
#             # copy the file to the csv-main folder and delete both the original file and the csv file from excel folder
#             s3.meta.client.copy(copy_source, source_bucket, 'csv-main/' + key)
#             s3.meta.client.delete_object(Bucket=source_bucket,Key=object_key)

#     except Exception as err:
#         print ("Error -"+str(err))

import boto3
import sys
import pantab
from tableauhyperapi import TableName

from s3path import S3Path


def lambda_handler(event, context):

    # TODO implement
    # Getting the source bucket from event
    #source_bucket = event['Records'][0]['s3']['bucket']['name']
    #print("The Source bucket is ", source_bucket)
    # Defining S3 Client
    #
    s3_client = boto3.client('s3')
    s3_bucket_name = 'guar-file-handler'
    s3 = boto3.resource('s3',
                        aws_access_key_id='AKIA4VUVOV4Q53TP5WML',
                        aws_secret_access_key='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')

    my_bucket = s3.Bucket(s3_bucket_name)
    bucket_list = []

    # Searching for files with prefix Hyper
    for file in my_bucket.objects.filter(Prefix='hyper'):
        file_name = file.key
        file_ext = file_name[-6:]
        # Cheking extension file and adding to a list
        if (file_ext == ".hyper"):
            bucket_list.append(file.key)
    print("if sys.version_info[0] > 3:")
    if sys.version_info[0] > 3:
        from io import StringIO  # Python 3.x
    if len(bucket_list) == 0:
        print("No files found in /hyper folder")
        exit()
    # Reading the files From the list created before
    for file in bucket_list:
        print("Creating object from s3 bucket")
        # Coping Hyper file from /hyper to tmp folder
        try:
            # get file name from the event
            #object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
            #file_name = str(object_key.split('/')[-1])
            print("The file name to download in tmp folder is", file_name)
            hyper_name = file_name.replace("hyper/", "")
            print("The name of the hyperfile is ", hyper_name)
            csv_name = hyper_name.replace(".hyper", ".csv")
            print("The name of the csv is "+csv_name)
            ###Saving a copy of the file in TMP Folder####
            try:
                table_name = TableName("Extract", "Extract")
                obj = s3.Object('guar-file-handler', file_name)
                data = obj.get()['Body'].read()
                url = 's3://guar-file-handler/hyper/dates.hyper'
                info = obj.meta.identifiers
                path = S3Path.from_uri(url)
                print(path.bucket)
                '/bucket_name'
                print(path.key)
                'folder1/folder2/file1.json'

                copy_source = {'Bucket': 'guar-file-handler',
                               'Key': 'dates.hyper'}
                copy_dest = {'Bucket': 'guar-file-handler', 'Key': 'dates.csv'}
                df = pantab.frame_from_hyper(copy_source, table=table_name)
                print("Reading dataframe from copy source")
                df.to_csv(copy_dest)
                print("saving data frame to dest source")

            except Exception as err:
                print("Error Making a copy of"+file_name+" to tmp folder", err)

            ###Opening copy of the file in TMP Folder##
            ### and reading info into a dataframe  ####

            try:
                with open('/tmp/' + file_name, 'r') as data:
                    table_name = TableName("Extract", "Extract")
                    df = pantab.frame_from_hyper(data, table=table_name)
                    try:
                        print(
                            "Trying to convert dataframe to csv directly into csv-main")
                        df.to_csv('/csv-main/' + csv_name)
                    except Exception as err:
                        print("Error saving csv file " + csv_name +
                              " into csv-main folder", err)

            except Exception as err:
                print("Error opening file "+file_name+" in read mode")

        except Exception as err:
            print("Error obtaining  " + object_key)


if __name__ == "__main__":
    lambda_handler('', '')
