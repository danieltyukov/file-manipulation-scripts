import boto3
import sys
import pantab
import os
from tableauhyperapi import TableName


def lambda_handler(event, context):

    # TODO implement
    s3_client = boto3.client('s3')
    s3_bucket_name = 'guar-file-handler'
    s3 = boto3.resource('s3',
                        aws_access_key_id='AKIA4VUVOV4Q53TP5WML',
                        aws_secret_access_key='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')
    zsource_bucket = event['Records'][0]['s3']['bucket']['name']

    my_bucket = s3.Bucket(s3_bucket_name)
    bucket_list = []
    #Searching for files with prefix Hyper
    for file in my_bucket.objects.filter(Prefix='hyper'):
        file_name = file.key
        file_ext = file_name[-6:]
        #Cheking extension file and adding to a list
        if (file_ext==".hyper"):
            bucket_list.append(file.key)
    print("if sys.version_info[0] > 3:")
    if sys.version_info[0] > 3:
        from io import StringIO  # Python 3.x
    if len(bucket_list) == 0:
        print("No files found in /hyper folder")
        exit()
    #Reading the files From the list created before
    for file in bucket_list:
        print("Creating object from s3 bucket")
        #Coping Hyper file from /hyper to tmp folder
        try:
            hyper_name = file_name.replace("hyper/", "")
            csv_name = hyper_name.replace(".hyper", ".csv")
            s3_file_to_download = 'hyper/'+hyper_name
            file_dest = s3_bucket_name+hyper_name
            s3_bucket_name = 'guar-file-handler'
            s3_client.download_file(
                Bucket=s3_bucket_name, Key=s3_file_to_download, Filename=file_dest
            )
            print("Making a copy of"+hyper_name + " from ./hyper folder to tmp folder")
        except Exception as err:
            print("Error Making a copy from hyper folder to tmp folder", err)

        # Opening information from Hyper file located in hyper folder
        try:
            file_to_get = 'hyper/'+hyper_name
            hyperfile = s3.Object('guar-file-handler', file_to_get).get()
        except Exception as err:
            print("Error Obtaining information of data from file"+file_to_get, err)
        # Reading data from hyper file
        try:
            print("Obtaining Hyper File "+file_to_get + " from /hyper folder")
            data = hyperfile['Body'].read()

        except Exception as err:
            print("Reading data from Hyper File " + file_to_get,err)
            tmp_hyper_name= s3_bucket_name, 'tmp/' + hyper_name
            print("Referencing tmp folder to create file"+tmp_hyper_name)
        # Creating new file in TMP FOLDER
        try:
            tmp_hyper_name = s3_bucket_name + hyper_name
            with open(s3_bucket_name, 'tmp/', 'wb') as f:
                print("Writing data into tmp file")
                f.write(data)
                f.close()
                print("File created in" + tmp_hyper_name)
        except Exception as err:
            print("Error Saving data from hyperfile from s3 Bucket", err)
    try:
        table_name = TableName("Extract", "Extract")
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        aux = (s3_bucket_name, 'tmp/' +hyper_name)
        df = pantab.frame_from_hyper(aux, table=table_name)
        print("Reading data frame")
    except Exception as err:
        print("Error reading hyperfile from file to data frame" + str(err))

    try:
        print("converting data frame to csv")
        csv_file_path = '/tmp/'+csv_name
        df.to_csv(csv_file_path)
    except Exception as err:
        print("Error Trying to csv in tmp file" + str(err))

    #Coping csv from tmp to csv-main
    copy_source = {'Bucket': 'guar-file-handler', 'Key': csv_file_path}
    try:
        s3.meta.client.copy(copy_source, 'guar-file-handler', 'csv-main/' + csv_name)
        #Deleting Hyper from tmp
        try:
            s3.Object('guar-file-handler', 'tmp/'+hyper_name).delete()
            print("TMP File "+hyper_name+" deleted from hyper/")
        except Exception as err:
            print("Error deleting hyper" + hyper_name + "from tmp", str(err))
        #Deleting csv from tmp
        try:
            s3.Object('guar-file-handler', 'tmp/'+csv_name).delete()
            print("TMP File "+csv_name+" deleted from hyper/")
        except Exception as err:
            print("Error deleting CSV" + csv_name + "from tmp", str(err))

    except Exception as err:
        print("Error coping csv " + csv_name + "from main to /csvmain", str(err))

    #
    # except Exception as err:
    #     print("Error Coping csv from tmp to main-csv" + str(err))
    #
    # try:
    #     csv_file_path = 'tmp/' + csv_name
    #     s3_client.upload_file(
    #         Bucket=s3_bucket_name, Key='csv-main/dates.csv', Filename=csv_file_path
    #     )
    #     print("Uploading csv file in /csv-main folder")
    #     try:
    #         s3.Object('guar-file-handler', 'hyper/'+hyper_name).delete()
    #         print("File "+file_name+" deleted from hyper/")
    #     except Exception as err:
    #         print("Error reading data frame-" + str(err))
    #
    #
    #
    # except Exception as err:
    #     print("Error reading hyperfile from file to data frame" + str(err))


#
if __name__ == "__main__":
    lambda_handler('','')