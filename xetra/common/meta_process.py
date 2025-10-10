import logging
import os
from io import StringIO, BytesIO
import boto3
import pandas as pd
from xetra.common.s3 import S3BucketConnector

class MetaProcess(self):
    """
    class for working with the meta file
    """

    @staticmethod
    def update_meta_file(extract_date_list: list, meta_key: str, s3_bucket_meta: S3BucketConnector):
        """
        Updateing the meta file with the processed Xetra dates and todays date as processed date

        :param: extract_date_list -> a list of dates that are extracted from the source
        :param: meta_key -> key of the meta file on the S3 bucket
        :param: s3_bucket_meta -> S3BucketConnector for the meta
        """
        #Creating an empty DataFrame using the meta file column names
        df_new = pd.DataFrame(columns = [
            MetaProcessFormat.META_SOURCE_DATE_COL.value
        ])

    @staticmethod
    def return_date_list():
        pass