from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (CreateTablesOperator,StageToRedshiftOperator,
                               LoadFactOperator, LoadDimensionOperator,
                               DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'Evelyn',
    'start_date': datetime(2019, 1, 12),
    'end_date': datetime(2019, 1, 13),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}

dag = DAG('udacity-pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
         )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Create the songplays fact table
create_fact_tables = CreateTablesOperator(
    task_id='Create_fact_tables',
    tables=['songplays'],
    redshift_conn_id='redshift',
    sql_queries=[SqlQueries.songplay_table_create],
    dag=dag
)

# Create the dimension tables: artists, songs, time, and users
create_dimension_tables = CreateTablesOperator(
    task_id='Create_dimension_tables',
    tables=['artists', 'songs', 'time', 'users'],
    redshift_conn_id='redshift',
    sql_queries=[SqlQueries.artist_table_create,
                 SqlQueries.song_table_create,
                 SqlQueries.time_table_create,
                 SqlQueries.user_table_create],
    dag=dag
)

# Create the two stagginng tables: staging events and staging songs
create_staging_tables = CreateTablesOperator(
    task_id='Create_staging_tables',
    tables=['staging_events', 'staging_songs'],
    redshift_conn_id='redshift',
    sql_queries=[SqlQueries.staging_event_table_create,
                 SqlQueries.staging_song_table_create],
    dag=dag
)


# Copy the log_data from S3 to Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    provide_context=True,
    dag=dag
)

# Copy the song_data from S3 to Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    table="staging_songs",
    s3_bucket='udacity-dend',
    s3_key='song_data',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    provide_context=True,
    dag=dag
)

# Populate data into songplays table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    table='songplays',
    dag=dag
)

# Populate data into users table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    table='users',
    dag=dag
)

# Populate data into songs table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    table='songs',
    dag=dag
)

# Populate data into artists table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    table='artists',
    dag=dag
)

# Populate data into time table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    table='time',
    dag=dag
)

# Ensure the data quality in all new 5 tables created
# in Redshift
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)


