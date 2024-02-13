# -*- coding: utf-8 -*-
"""
Created on Sun Jan 17 19:47:00 2021

@author: priya marimuthu
"""
import boto3
from botocore.exceptions import NoCredentialsError


class S3QueryEngine(object):

    def create_s3_bucket(self, bucket_name, access_key, secret_key):
        """
        Function to create new bucket in S3.

        :param bucket_name: The name of s3 bucket to be created
        :param access_key: The access key to connect with AWS S3
        :param secret_key: The secret key to connect with AWS S3
        :return: returns nothing
        """

        s3 = boto3.client('s3', aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key)
        try:
            s3.create_bucket(Bucket=bucket_name)
            print("s3 Bucket with name %s created successfully" % bucket_name)
        except:
            print("s3 Bucket with name %s already exists!" % bucket_name)

    def upload_to_aws_s3(self, local_file, bucket, s3_file, access_key, secret_key):
        """
        Function to upload file into bucket in S3.
        
        :param local_file: The path of the file to be uploaded
        :param bucket: The name of s3 bucket to which the file needs to be uploaded
        :param s3_file: The name in which the s3 file needs to be uploaded
        :param access_key: The access key to connect with AWS S3
        :param secret_key: The secret key to connect with AWS S3
        :return: returns nothing
        """

        s3 = boto3.client('s3', aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key)
        try:
            s3.upload_file(local_file, bucket, s3_file, ExtraArgs={"ServerSideEncryption": "aws:kms",
                                                                   'SSEKMSKeyId': 'alias/aws/s3'})
            print("Upload is successful")
        except FileNotFoundError:
            print("File is not found")
        except NoCredentialsError:
            print("Credentials not available")

    def create_database(self, database_name, bucket_name, region, access_key, secret_key):
        """
        Function to create database in Athena.
        
        :param database_name: The name of the database to be created
        :param bucket_name: The name of s3 bucket to which the output results of the query are sent
        :param region: The AWS region used to instantiate the client 
        :param access_key: The access key to connect with AWS S3
        :param secret_key: The secret key to connect with AWS S3
        :return: returns nothing
        """

        client = boto3.client('athena', aws_access_key_id=access_key,
                              aws_secret_access_key=secret_key, region_name=region)

        response = client.start_query_execution(QueryString='create database if not exists' + ' ' + database_name,
                                                ResultConfiguration={'OutputLocation': 's3://' + bucket_name})
        print(response)

    def create_table(self, database_name, table_name, query, region, bucket_name, access_key, secret_key):
        """
        Function to create table in Athena to give access to records inside file in s3  
        
        :param database_name: The name of the database in which the table needs to be created
        :param table_name: The name of the table to be create
        :param query: The query required to create the table
        :param region: The AWS region used to instantiate the client 
        :param bucket_name: The name of s3 bucket to which the output results of the query are sent
        :param access_key: The access key to connect with AWS S3
        :param secret_key: The secret key to connect with AWS S3
        :return: returns nothing
        """

        client = boto3.client('athena', aws_access_key_id=access_key,
                              aws_secret_access_key=secret_key, region_name=region)
        response = client.start_query_execution(QueryString=query,
                                                QueryExecutionContext={'Database': database_name},
                                                ResultConfiguration={'OutputLocation': 's3://' + bucket_name})
        print(response)

    def add_partition(self, database_name, table_name, yyyy, mm, dd, s3_file_location, region, bucket_name, access_key,
                      secret_key):
        """
        Function to add partition table based on year/month/day in athena  
        
        :param database_name: The name of the database which contains the table to be partitioned
        :param table_name: The name of the table to be partitioned
        :param yyyy: The year used to partition data in the table
        :param mm: The month used to partition data in the table
        :param dd: The day used to partition data in the table
        :param s3_file_location: Location of s3 file/bucket that contains data to be partitioned
        :param region: The AWS region used to instantiate the client 
        :param bucket_name: The name of s3 bucket to which the output results of the query are sent
        :param access_key: The access key to connect with AWS S3
        :param secret_key: The secret key to connect with AWS S3
        :return: returns nothing
        """

        client = boto3.client('athena', aws_access_key_id=access_key,
                              aws_secret_access_key=secret_key, region_name=region)
        response = client.start_query_execution(
            QueryString='ALTER TABLE' + ' ' + database_name + '.' + table_name + 
                        ' ' + """ADD PARTITION (year = '""" + yyyy + """', month ='""" + mm + """', day ='""" + dd + 
                        """')""" + ' ' + 'location' + """ '""" + s3_file_location + """'""",
            QueryExecutionContext={'Database': database_name},
            ResultConfiguration={'OutputLocation': 's3://' + bucket_name})
        print(response)

    def get_number_of_records(self, database_name, table_name, region, bucket_name, access_key, secret_key):
        """
        Function to get the number of records in the table 
        
        :param database_name: The name of the database which contains the table to be queried
        :param table_name: The name of the table to be queried
        :param region: The AWS region used to instantiate the client 
        :param bucket_name: The name of s3 bucket to which the output results of the query are sent
        :param access_key: The access key to connect with AWS S3
        :param secret_key: The secret key to connect with AWS S3
        :return: returns nothing
        """

        client = boto3.client('athena', aws_access_key_id=access_key,
                              aws_secret_access_key=secret_key, region_name=region)
        response = client.start_query_execution(
            QueryString='select count(*) from' + ' ' + database_name + '.' + table_name,
            QueryExecutionContext={'Database': database_name},
            ResultConfiguration={'OutputLocation': 's3://' + bucket_name})
        print(response)


if __name__ == '__main__':
    # access key and secret key to access AWS s3
    access_key = 'XXXXXXXXXXXXXXXX'
    secret_key = 'XXXXXXXXXXXXXXXXX'
    # create object of S3QueryEngine class
    obj = S3QueryEngine()
    # create new S3 bucket
    obj.create_s3_bucket('priya008', access_key, secret_key)
    # upload file to S3 bucket
    obj.upload_to_aws_s3('part-00000-4dd69b87-151c-40c8-9c4f-c20a980920e2-c000.snappy.parquet', 'priya008',
                         'part-00000-4dd69b87-151c-40c8-9c4f-c20a980920e2-c000.snappy.parquet', access_key, secret_key)
    # create new database in Athena
    obj.create_database('TestDB', 'priya008', 'us-east-1', access_key, secret_key)
    # create new table in Athena to give access to records in s3 bucket
    obj.create_table('TestDB', 'AmplitudeEventLogs', '''CREATE external TABLE if not exists TestDB.AmplitudeEventLogs(
        adid string,
        amplitude_attribution_ids string,
        amplitude_event_type string,
        amplitude_id INT,
        city STRING,
        client_event_time date,
        client_upload_time date,
        country STRING,
        device_brand STRING,
        device_carrier string,
        device_family string,
        device_manufacturer string,
        device_model STRING,
        device_type STRING,
        dma STRING,
        event_id int,
        event_time date,
        event_type string,
        idfa string,
        is_attribution_event boolean,
        language string,
        library STRING,
        location_lat float,
        location_lng float,
        data string,
        os_name STRING,
        os_version STRING,
        paying boolean,
        platform STRING,
        processed_time date,
        region string,
        sample_rate string,
        server_received_time date,
        server_upload_time date,
        start_version STRING,
        user_creation_time date,
        version_name STRING
        )
        PARTITIONED BY (YEAR STRING, MONTH STRING, DAY STRING)
        STORED AS PARQUET
        LOCATION 's3://priya008/'
        tblproperties ("parquet.compression"="SNAPPY");''', 'us-east-1', 'priya008', access_key, secret_key)
    # add new partition based on year,month and day to table in athena
    obj.add_partition('TestDB', 'AmplitudeEventLogs', '2019', '10', '29', 's3://priya008/2019/10/29', 'us-east-1',
                      'priya008', access_key, secret_key)
    # get number of records in the table
    obj.get_number_of_records('TestDB', 'AmplitudeEventLogs', 'us-east-1', 'priya008', access_key, secret_key)
