import os
import configparser
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from copy_to_redshift import CopyToRedshiftOperator
from sas_to_redshift import SASToRedshiftOperator
from data_quality import DataQualityOperator

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col, count, rand, isnan, when

from sql_queries_create import SqlQueriesCreate
from table_configs import s3_table_keys, sas_table_configs

config = configparser.ConfigParser()
config.read('/home/pi/4-capstone/i94.cfg')

S3_BUCKET = config['S3']['BUCKET']
REDSHIFT_ARN = config['IAM_ROLE']['ARN']

default_args = {
    'owner': 'robrobinson',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12, 0, 0, 0, 0),
    'end_date': datetime(2019, 1, 12, 0, 0, 0, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('i94_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
dim_tables_created = DummyOperator(task_id='DimTables_created',  dag=dag)
staging_tables_created = DummyOperator(task_id='StagingTables_created',  dag=dag)
tables_copied = DummyOperator(task_id='Tables_copied',  dag=dag)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

### CREATE DIM TABLES
### Execute Create-tables Queries directly with Postgres
for table in SqlQueriesCreate.dim_tables.keys():
    create_table_task = PostgresOperator(
            task_id=f"create_{table}_table",
            postgres_conn_id="redshift",
            sql=SqlQueriesCreate.dim_tables[table],
            dag=dag
        )
    
    start_operator >> create_table_task
    create_table_task >> dim_tables_created

### CREATE STAGING TABLES
for table in SqlQueriesCreate.staging_tables.keys():
    create_table_task = PostgresOperator(
            task_id=f"create_{table}_table",
            postgres_conn_id="redshift",
            sql=SqlQueriesCreate.staging_tables[table],
            dag=dag
        )
    
    dim_tables_created >> create_table_task
    create_table_task >> staging_tables_created

### COPY DATA TO STAGING TABLES
for table in s3_table_keys:
    copy_table_from_s3_to_redshift = CopyToRedshiftOperator(
        task_id=f'load_{table["name"]}_from_s3',
        dag=dag,
        aws_credentials_id='aws_credentials',
        redshift_conn_id='redshift',
        iam_role=REDSHIFT_ARN,
        table=table['name'],
        s3_bucket=S3_BUCKET,
        s3_key=table['key'],
        file_format=table['file_format'],
        delimiter=table['sep']
      )
    
    staging_tables_created >> copy_table_from_s3_to_redshift
    copy_table_from_s3_to_redshift >> tables_copied
    
### LOAD DATA INTO DIM TABLES
for table in sas_table_configs:
  load_table_from_sas = SASToRedshiftOperator(
    task_id=f'load_{table["name"]}_from_sas',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    table=table['name'],
    s3_bucket=S3_BUCKET,
    s3_key='i94_data/I94_SAS_Labels_Descriptions.SAS',
    sas_value=table['value'],
    columns=table['columns']
  )

  check_table = DataQualityOperator(
    task_id=f'qc_{table["name"]}_table',
    dag=dag,
    redshift_conn_id='redshift',
    table=table['name'],
    dq_checks=table['qc_check']
  )
  
  tables_copied >> load_table_from_sas
  load_table_from_sas >> check_table
  check_table >> end_operator
