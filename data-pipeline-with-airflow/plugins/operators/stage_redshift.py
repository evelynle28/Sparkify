from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    A custom operator that will copy the data
    from a provided S3 bucket to the Redshift
    cluster connected
    """

    ui_color = '#358140'

    copy_sql = '''
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
    '''

    @apply_defaults
    def __init__(self,
                 table='',
                 redshift_conn_id='',
                 aws_credentials_id='',
                 s3_bucket='',
                 s3_key='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key


    def execute(self, context):

        aws = AwsHook(self.aws_credentials_id)
        credentials = aws.get_credentials()

        redshift = PostgresHook(self.redshift_conn_id)

        self.log.info(f'Clearing all existing records in table {self.table}')
        redshift.run(f'TRUNCATE TABLE {self.table}')

        self.log.info('Copying data from S3 to Redshift')
        query = StageToRedshiftOperator.copy_sql.format(
            self.table,
            f's3://{self.s3_bucket}/{self.s3_key}',
            credentials.access_key,
            credentials.secret_key,
        )

        redshift.run(query)
        self.log.info(f'Successfully copied {self.table} from S3 to Redshift')






