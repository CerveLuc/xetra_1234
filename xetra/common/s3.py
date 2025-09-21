"""Connector and methods for accessing S3"""

import os
import boto3

class S3BucketConnector():
    """
    Class for interacting with S3 buckets
    """
    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str)
        """
        Constructor for S3BucketConnector

        :param acces_key: access key for accessing S3
        :param secret_key: secret key for accessing S3
        :param endpoint_url: endpoint url to S3
        :param bucket: S3 Bucket name
        """

        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id = os.environ[access_key],
                                     aws_secret_access_key = os.environ[secret_key])
        # one underline means that it is a protected variables, 2 underlines would mean that it is a private variables
        self._s3 = self.session.resource(service_name = 'S3', endpoint_url = endpoint_url)
        self._bucket = self._s3_.Bucket(bucket)

    def list_files_in_prefix(self):
        pass
    
    def read_csv_to_df(self):
        pass

    def write_df_to_s3(self):
        pass