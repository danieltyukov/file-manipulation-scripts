import psycopg2
import boto3
import urllib


def lambda_handler(event, context):

    # when CSV file lands in the bucket folder its name must be taken.
    # the file name must be used to create a table in Redshift.
    # the file must be copied to the Redshift database.
    # the file must be deleted from the bucket.
    # if the file cannot be copied to Redshift, it must be moved to the unproucessed folder.

    # folder name: csv-main

    s3 = boto3.resource('s3')
    source_bucket = event['Records'][0]['s3']['bucket']['name']

    # get file name
    object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    file_name = str(object_key.split('/')[-1])

    # generate create table statement out of the csv file
    create_table_statement = "CREATE TABLE IF NOT EXISTS " + file_name + " ("
    with open('/tmp/' + file_name, 'wb') as data:
        s3.Bucket(source_bucket).download_fileobj(object_key, data)
    with open('/tmp/' + file_name, 'r') as data:
        header = data.readline().split(',')
        for column in header:
            create_table_statement += column + " varchar(255),"
        create_table_statement = create_table_statement[:-1] + ");"
    
    print(create_table_statement)

    # connect to Redshift
    conn = psycopg2.connect(dbname='dev', host='redshift-cluster-1.cqjzjxqjxqjx.us-east-1.redshift.amazonaws.com', port='5439', user='awsuser', password='Password1')
    cur = conn.cursor()

    # create table in Redshift
    cur.execute(create_table_statement)
    conn.commit()
        

     