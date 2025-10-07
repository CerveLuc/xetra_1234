"""Connector and methods for accessing S3"""
import logging
import os
from io import StringIO, BytesIO
import boto3
import pandas as pd
import xetra.common.custom_exceptions

class S3BucketConnector():
    """
    Class for interacting with S3 buckets
    """
    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str):
        """
        Constructor for S3BucketConnector

        :param acces_key: access key for accessing S3
        :param secret_key: secret key for accessing S3
        :param endpoint_url: endpoint url to S3
        :param bucket: S3 Bucket name
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id = os.environ[access_key],
                                     aws_secret_access_key = os.environ[secret_key])
        # one underline means that it is a protected variables, 2 underlines would mean that it is a private variables
        self._s3 = self.session.resource(service_name = 'S3', endpoint_url = endpoint_url)
        self._bucket = self._s3.Bucket(bucket)

    def list_files_in_prefix(self, prefix: str):
        """
        Listing all files with a prefix on the S3 Bucket

        :param prefix: prefix on the S3 Bucket that should be filtered with

        returns:
            files: list of all the files name containing the prefix in the key
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files

    def read_csv_to_df(self, key: str, decoding: str = 'utf-8', sep: str = ','):
        """
        reading a csv file from the S3 bucket and returning a dataframe

        :param key: key of the file that should be read
        :param encoding: encoding of the data inside the csv file
        :param sep: seperator of the csv file

        returns:
            data_frame: Pandas Dataframe containing the data of the csv file
        """
        self._logger('Reading file %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(decoding)
        data = StringIO(csv_obj)
        df = pd.read_csv(data, delimiter=sep)
        return df

    def write_df_to_s3(self, data_frame: pd.DataFrame, key:str, file_format: str):
        """
        writing a Pandas DataFrame to S3
        supported formats: .csv, .parquet

        :data_frame: Pandas DataFrame that should be writenn
        :key: target key of the saved file
        :file_format: format of the saved file
        """
        if data_frame.empty():
            self._logger.info('The dataframe is empty! Now file will be written..')
            return None
        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        self._logger.info('The file format %s is not :'
                         'supported to be written to s3!', file_format)
        raise WrongFormatException
    
    
    def __put_object(self, out_buffer: StringIO or BytesIO, key: str):
        """
        Helper function for self.write_df_to_s3()

        :out_buffer: StringIO  or BytesIO that should be written
        :key: target key of the saved file
        """
        self._logger.info('Writing the file to %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        self._bucket.put_object(Body=out_buffer.get_value(), Key=key)
        return True