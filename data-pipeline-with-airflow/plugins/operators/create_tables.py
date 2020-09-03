from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os


class CreateTablesOperator(BaseOperator):
    """
    A custom operator that will create tables
    in the Redshift cluster connected
    """
    
    ui_color = '#E4F584'
        
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 tables=[],
                 sql_queries=[],
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.sql_queries= sql_queries

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info('Redshift Hook created')
        
        for i, query in enumerate(self.sql_queries):
            redshift.run(query)
            self.log.info(f'Table {self.tables[i]} created')
            
        
                
        
        
        
 