import boto3
import pandas as pd

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.hooks.base_hook import BaseHook
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook

from sqlalchemy import create_engine, text


class SASToRedshiftOperator(BaseOperator):
    """Operator to load data from SAS labels to tables.
    """
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 sas_value="",
                 columns="",
                 *args, **kwargs):
        """Loads code:value labels from SAS labels file to Redshift table
        Args:
            aws_credentials_id (str): Airflow ID for AWS credentials.
            redshift_conn_id (str): Airflow ID for redshift connection.
            table (str): destination table name.
            s3_bucket (str): S3 Bucket for SAS labels.
            s3_key (str): S3 Key for SAS labels.
            sas_value (str): value in SAS labels to extract.
            columns (list): destination column names.
        """
        super(SASToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.sas_value = sas_value
        self.columns = columns
    
    def execute(self, context):
        """Executes task for staging to redshift.
        Args:
            context (:obj:`dict`): Dict with values to apply on content.
        Returns:
            None   
        """
        s3 = S3Hook(self.aws_credentials_id)

        redshift_conn = BaseHook.get_connection(self.redshift_conn_id)
        self.log.info('Connecting to {}...'.format(redshift_conn.host))
        conn = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
                             redshift_conn.login,
                             redshift_conn.password,
                             redshift_conn.host,
                             redshift_conn.port,
                             redshift_conn.schema
                            ))
        
        self.log.info(f'S3: s3://{self.s3_bucket}/{self.s3_key}')
        file_str = s3.read_key(self.s3_key, self.s3_bucket)
        self.log.info('File length: {}'.format(len(file_str)))
        self.log.info('SAS Value: {}'.format(self.sas_value))
        file_str = file_str[file_str.index(self.sas_value):]
        file_str = file_str[:file_str.index(';')]
        
        lines = file_str.split('\n')[1:]
        self.log.info('Number of Lines: {}'.format(len(lines)))
        codes = []
        values = []
        
        self.log.info('Parsing SAS file: {}/{}'.format(self.s3_bucket, self.s3_key))
        for line in lines:
            if '=' in line:
                code, val = line.split('=')
                code = code.strip()
                val = val.strip()
                
                if code[0] == "'":
                    code = code[1:-1]
                    
                if val[0] == "'":
                    val = val[1:-1]
                    
                codes.append(code)
                values.append(val)

        self.log.info(f'Codes: {len(code)}')
        self.log.info(f'Values: {len(code)}')
        self.log.info('Creating dataframe...')
        df = pd.DataFrame(list(zip(codes, values)), columns=self.columns)
        self.log.info('DataFrame: {}'.format(df.info()))
        self.log.info('DataFrame Head: {}'.format(df.head(5)))

        truncate_query = text(f'TRUNCATE TABLE {self.table}')
        conn.execution_options(autocommit=True).execute(truncate_query)

        self.log.info('Writing table {}'.format(self.table))
        df.to_sql(self.table, conn, index=False, if_exists='append')
        conn.dispose()
