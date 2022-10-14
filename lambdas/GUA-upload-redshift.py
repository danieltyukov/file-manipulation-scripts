import psycopg2
import boto3
import urllib
import re
import os


def lambda_handler(event, context):
    try:

        passw = os.environ['pass']
        aws_access_key_id = os.environ['aws_access_key_id']
        aws_secret_access_key = os.environ['aws_secret_access_key']

        # folder name: csv-main
        s3 = boto3.resource('s3')
        source_bucket = event['Records'][0]['s3']['bucket']['name']

        # get file name
        object_key = urllib.parse.unquote_plus(
            event['Records'][0]['s3']['object']['key'])
        file_name = str(object_key.split('/')[-1])

        table_name = re.sub('[^a-zA-Z0-9 \n\.]', '', file_name.split('.')[0])
        table_name = table_name.replace(' ', '')

        # get schema name
        if '--' in file_name:
            schema_name = file_name.split('--')[0]
            schema_name = re.sub('[^a-zA-Z0-9 \n\.]', '', schema_name)
            schema_name = schema_name.replace(' ', '')
        else:
            schema_name = file_name.split('.')[0]
            schema_name = re.sub('[^a-zA-Z0-9 \n\.]', '', schema_name)
            schema_name = schema_name.replace(' ', '')
        
        # print schema name
        print('Schema Name: ' + schema_name)

        create_table_statement = "CREATE TABLE IF NOT EXISTS " + \
            table_name + " ("
        with open('/tmp/' + file_name, 'wb') as data:
            s3.Bucket(source_bucket).download_fileobj(object_key, data)
        with open('/tmp/' + file_name, 'r') as data:
            header = data.readline().split(',')
            # find column types

            for i in range(len(header)):
                header[i] = re.sub('[^a-zA-Z0-9 \n\.]', '', header[i])
                header[i] = header[i].replace(' ', '_')

                if (header[i] == " " or header[i] == ""):
                    continue
                
                create_table_statement += header[i] + " VARCHAR(255),"
                # data.seek(0)
                # data.readline()
                # for line in data:
                #     if (line.split(',')[i].isdigit()):
                #         create_table_statement += header[i] + " INT,"
                #         break
                #     elif (line.split(',')[i].replace('.', '', 1).isdigit()):
                #         create_table_statement += header[i] + " FLOAT,"
                #         break
                #     elif (line.split(',')[i].replace('/', '', 2).isdigit()):
                #         create_table_stppoatement += header[i] + " DATE,"
                #         break
                #     else:
                #         create_table_statement += header[i] + " VARCHAR(255),"
                #         break

            create_table_statement = create_table_statement[:-1] + ");"

        print("create table statement: ")
        print(create_table_statement)

        # connect to Redshift

        conn = psycopg2.connect(dbname='alpha', host='redshift-cluster-guardian.cqp1qbc3xi4l.ap-south-1.redshift.amazonaws.com',
                                port='5439', user='awsuser', password=passw, options='-c search_path=' + schema_name)
        cur = conn.cursor()

        # create a schema in redshift
        # create schema authorization
        cur.execute("CREATE SCHEMA IF NOT EXISTS " + schema_name +
                    " AUTHORIZATION awsuser;")
        conn.commit()

        # create table in Redshift
        cur.execute(create_table_statement)
        conn.commit()

        # fill in data to the table

        # copy statement
        copy_statement = "COPY " + table_name + " FROM 's3://" + source_bucket + "/" + "csv-main/" + file_name + "' CREDENTIALS 'aws_access_key_id=" + aws_access_key_id + \
            ";aws_secret_access_key=" + aws_secret_access_key + \
            "' DELIMITER ',' IGNOREHEADER 1 REGION 'ap-south-1' REMOVEQUOTES EMPTYASNULL BLANKSASNULL MAXERROR 5;"
        print("copy statement: ")
        print(copy_statement)

        # copy data to Redshift
        cur.execute(copy_statement)
        conn.commit()

        # delete file from S3
        s3.Object(source_bucket, object_key).delete()

    except Exception as e:
        print(e)
        s3.Object(source_bucket, 'unprocessed/' +
                  file_name).copy_from(CopySource=source_bucket + '/' + object_key)
        s3.Object(source_bucket, object_key).delete()
        raise e
