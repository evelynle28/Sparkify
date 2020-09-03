from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    A custom operator that will check if the data
    is properly inserted into tables
    """
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        for table in self.tables:
            records = redshift.get_records(f'SELECT COUNT(*) FROM {table}')
            
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error(f'Data quality check failed. {table} returned no results')
                raise ValueError(f'Data quality check failed. {table} returned no results')
                
            if records[0][0] < 1:
                self.log.error(f'Data quality check failed. {table} has no records')
                raise ValueError(f'Data quality check failed. {table} has no records')
            
            self.log.info(f'Data quality on table {table} check passed with {records[0][0]} records')