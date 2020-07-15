from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime

class CopyToRedshiftOperator(BaseOperator):
    """Operator to Copy data from S3 into Redshift.
    
        Attributes:
        copy_sql (string): The sql COPY command to be
            formatted in the 'execute' method.
    """
    
    ui_color = '#358140'
    
    copy_sql = """
        COPY {copy_sql}
        FROM '{from_sql}'
        {credentials}
        {format_sql}
        {ignore_headers_sql}
        {delimiter_sql}
        COMPUPDATE OFF;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 iam_role="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 file_format="csv",
                 ignore_headers=1,
                 delimiter=',',
                 *args, **kwargs):

        super(CopyToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.ignore_headers = ignore_headers
        self.delimiter = delimiter
        self.iam_role = iam_role
        
    def execute(self, context):
        """Copy data from S3 into staging table.
        """
        self.log.info("GATHERING CREDENTIALS AND PATHS...")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        self.log.info(f"RENDERED_KEY: {rendered_key}")
        self.log.info(f"S3_PATH: {s3_path}")
        
        redshift_hook.run('CREATE SCHEMA IF NOT EXISTS public')
        redshift_hook.run('DELETE FROM {}'.format(self.table))
        
        if self.file_format == 'csv':
            # If csv file, set delimiter and ignore headers.
            format_sql = "CSV"
            ignore_headers_sql = "IGNOREHEADER 1"
            delimiter_sql = f"DELIMITER '{self.delimiter}'"
            credentials_sql = f"ACCESS_KEY_ID '{credentials.access_key}' SECRET_ACCESS_KEY '{credentials.secret_key}'"
        else:
            # If a .json is passed, use for format: else default 'auto'
            format_sql = f"FORMAT AS PARQUET"
            credentials_sql = f"IAM_ROLE '{self.iam_role}'"
            ignore_headers_sql = ""
            delimiter_sql = ""
                      
        formatted_sql = CopyToRedshiftOperator.copy_sql.format(
            copy_sql=self.table,
            from_sql=s3_path,
            credentials=credentials_sql,
            format_sql=format_sql,
            ignore_headers_sql=ignore_headers_sql,
            delimiter_sql=delimiter_sql
        )
        
        self.log.info(f"RUNNING COPY TO {self.table}...")
        redshift_hook.run(formatted_sql)



