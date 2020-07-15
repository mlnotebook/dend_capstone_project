from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """Operator for data quality check on redshift tables.
    """
    
    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 qc_checks=[],
                 *args, **kwargs):
        """Args:
            redshift_conn_id (str): Airflow ID for redshift connection.
            table (str): Table to quality check.
            query (:obj:`str`, optional): QC Query.
            result (:obj:`str`, optional): Expected result

        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.qc_checks = qc_checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Collects the number of records
        records = redshift.get_records("""SELECT COUNT(*) 
FROM {}""".format(self.table))
        
        # Ensure that table returns results
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f'Error: No results returned for {self.table}')
        
        # Ensure that table has records
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f'Error: No records in {self.table}')
        
        for check in self.qc_checks:
            sql = check.get('qc_sql')
            exp_result = check.get('expected_result')

            records = redshift.get_records(sql)[0]

            if exp_result != records[0]:
                raise ValueError('Quality check failed')
