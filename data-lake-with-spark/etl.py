import configparser
from datetime import datetime
import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as _struct, StructField as _field, DateType as _date,\
                              DoubleType as _double, IntegerType as _int,StringType as _str,\
                              TimestampType as _timestamp


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    '''
    Create the spark session with customized configuration
    to boost the reading/writng speed
    '''

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return spark


def get_log_schema():
    '''
    Specify the schema of the user_logs table
    to help read logs-data folder
    '''

    user_logs_schema = _struct([
        _field('artist', _str()),
        _field('auth', _str()),
        _field('firstName', _str()),
        _field('gender', _str()),
        _field('itemInSession', _int()),
        _field('lastName', _str()),
        _field('length', _double()),
        _field('level', _str()),
        _field('location', _str()),
        _field('method', _str()),
        _field('page', _str()),
        _field('registration', _str()),
        _field('sessionId', _int()),
        _field('song', _str()),
        _field('ts', _int()),
        _field('userAgent', _str()),
        _field('userId', _str())
    ])

    return user_logs_schema


def get_song_schema():
    '''
    Specify the songs_meta table to
    help read data from songs-data folder
    '''

    songs_meta_schema = _struct([
        _field('artist_id', _str()),
        _field('artist_latitude', _double()),
        _field('artist_location',_str()),
        _field('artist_longitude', _double()),
        _field('artist_name', _str()),
        _field('duration', _double()),
        _field('num_songs', _int()),
        _field('song_id', _str()),
        _field('title', _str()),
        _field('year', _int())
    ])

    return songs_meta_schema

def read_data(spark, input_data, song_subdir, log_subdir):
    '''
    Read data from input S3 bucket

    '''
    # get filepath to song and log data file
    song_data = f'{input_data}/{song_subdir}'
    log_data = f'{input_data}/{log_subdir}'


    # read song data file
    print(f'Reading from {song_subdir} ...')
    t0 = time.time()

    song_df = spark.read.json(song_data, schema=get_song_schema())

    print(f'Success! Total time: {time.time() - t0} sec \n')


    # read log data file
    print(f'Reading from {log_subdir} ...')
    t0 = time.time()

    log_df = spark.read.json(log_data, schema=get_log_schema())

    print(f'Success! Total time: {time.time() - t0} sec \n')


    return song_df, log_df


def process_song_data(spark, song_df, output_data):
    '''
    Function read in song dataframe, extract necessary columns
    to create songs and artist table, then write the tables to
    parquet files with songs table artitioned by year and artist

    Arguments:
        - spark: The current spark session
        - song_df: The song dataframe read from song_data folder
        - output_data: The bucket to store resulted parquet files

    Return:
        None

    '''

    ############################# SONG TABLE ################################

    print('Querying for songs table ...')
    t0 = time.time()

    # extract columns to create songs table
    songs_table = song_df.select(['song_id',\
                                  'title',\
                                  'artist_id',\
                                  'year',\
                                  'duration'])\
                        .dropDuplicates()\
                        .filter((col('song_id').isNotNull())\
                                 & (col('song_id')!= ''))

    print(f'Success! Total time: {time.time() - t0} sec \n')

    ####################### WRITING TO PARQUET FILES ##########################

    print('Writing song table...')
    t0 = time.time()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write\
               .partitionBy('year', 'artist_id')\
               .parquet(f'{output_data}/songs', 'overwrite')

    print(f'Success! Total time: {time.time() - t0} sec \n')

    ############################# ARTIST TABLE ################################

    print('Querying for artist table ...')
    t0 = time.time()

    # extract columns to create artists table
    artists_table = song_df.select(['artist_id',\
                               col('artist_name').alias('name'),\
                               col('artist_location').alias('location'),\
                               col('artist_latitude').alias('latitude'),\
                               col('artist_longitude').alias('longitude')])\
                    .dropDuplicates()\
                    .filter((col('artist_id').isNotNull()) & (col('artist_id') != ''))

    print(f'Success! Total time: {time.time() - t0} sec \n')

    ####################### WRITING TO PARQUET FILES ##########################

    print('Writing artist table ...')
    t0 = time.time()

    # write artists table to parquet files
    artists_table.write\
                 .parquet(f'{output_data}/artists', 'overwrite')

    print(f'Success! Total time: {time.time() - t0} sec \n')


def process_log_data(spark, log_df, song_df, output_data):
    '''
    Function extracts data from log dataframe to create users and time
    tables. It also read in song dataframe to combine the information
    and create the songplays fact table. All tables are then written
    into parquet files in output bucket and get partitioned properly.

    Argument:
        - spark: The current spark session
        - song_df: The song dataframe containing data read from song_data
        - log_df: The log dataframe containing data read from log_data
        - output_data: The bucket to store resulted parquet files

    Return:
        None
    '''

    ########################## USERS TABLE #####################################

    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')

    print('Querying for users table ...')
    t0 = time.time()

    # extract columns for users table
    users_table = log_df.select([col('userId').alias('user_id'),\
                             col('firstName').alias('first_name'),\
                             col('lastName').alias('last_name'),\
                             'gender',\
                             'level'])\
                    .dropDuplicates()\
                    .filter(col('user_id').isNotNull() & (col('user_id') != ''))

    print(f'Success! Total time: {time.time() - t0} sec \n')

     ####################### WRITING TO PARQUET FILES ##########################

    print('Writing users table...')
    t0 = time.time()

    # write users table to parquet files
    users_table.write\
               .parquet(f'{output_data}/users', 'overwrite')

    print(f'Success! Total time: {time.time() - t0} sec \n')

    ############################## TIME TABLE ##################################

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0), _timestamp() )
    log_df = log_df.withColumn('timestamp', get_timestamp(log_df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000.0), _date())
    log_df = log_df.withColumn('datetime', get_datetime(log_df.ts))


    print('Querying for time table ...')
    t0 = time.time()

    # extract columns to create time table
    time_table = log_df.select(col('timestamp').alias('start_time'),\
                           hour(col('datetime')).alias('hour'),\
                           dayofmonth(col('datetime')).alias('day'),\
                           weekofyear(col('datetime')).alias('week'),\
                           month(col('datetime')).alias('month'),\
                           year(col('datetime')).alias('year'),\
                           date_format(col('datetime'),'u').alias('weekday'))\
                  .dropDuplicates()\
                  .filter(col('timestamp').isNotNull())

    print(f'Success! Total time: {time.time() - t0} sec \n')

    ####################### WRITING TO PARQUET FILES ##########################

    print('Writing time table...')
    t0 = time.time()

    # write time table to parquet files partitioned by year and month
    time_table.write\
              .partitionBy('year', 'month')\
              .parquet(f'{output_data}/time', 'overwrite')

    print(f'Success! Total time: {time.time() - t0} sec \n')

    ########################## SONGPLAYS TABLE #################################

    # Create temporary view to join tables
    song_df.createOrReplaceTempView('song_data')
    log_df.createOrReplaceTempView('log_data')
    time_table.createOrReplaceTempView('time_table')

    print('Querying for songplays table ...')
    t0 = time.time()

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("""
        SELECT t.start_time as start_time,
               l.userId as user_id,
               l.level as level,
               s.song_id as song_id,
               s.artist_id as artist_id,
               l.sessionId as session_id,
               s.artist_location as artist_location,
               l.userAgent as user_agent,
               t.year as year,
               t.month as month
        FROM song_data s
        JOIN log_data l
            ON s.artist_name = l.artist
            AND s.duration = l.length
            AND s.title = l.song
        JOIN time_table t
            ON l.timestamp = t.start_time
    """)

    songplays_table.createOrReplaceTempView('songplays')
    songplays_table = spark.sql('select row_number() over (order by start_time) as songplay_id, * from songplays')

    print(f'Success! Total time: {time.time() - t0} sec \n')

    ####################### WRITING TO PARQUET FILES ##########################

    print('Writing songplays table...')
    t0 = time.time()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write\
                   .partitionBy('year', 'month')\
                   .parquet(f'{output_data}/songplays', 'overwrite')

    print(f'Success! Total time: {time.time() - t0} sec \n')


def main():
    start = time.time()

    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "<put your s3a://<bucket-name> here>"
    song_subdir = 'song_data/*/*/*/*.json'
    log_subdir = 'log_data/*/*/*.json'

    song_df, log_df = read_data(spark, input_data, song_subdir, log_subdir)
    process_song_data(spark, song_df, output_data)
    process_log_data(spark, log_df, song_df, output_data)

    print(f"TOTAL TIME FOR ETL: {(time.time() - start)/60.0} mins")

    spark.stop()

if __name__ == "__main__":
    main()
