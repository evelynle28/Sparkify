import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# ------ DROP TABLES ------ 
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# ------ CREATE TABLES ------ 
# Create staging tables
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession integer,
        lastName varchar,
        length double precision,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration double precision,
        sessionId integer,
        song varchar,
        status integer,
        ts bigint,
        userAgent varchar,
        userId integer
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id varchar,
        num_songs integer,
        title varchar,
        artist_name varchar,
        artist_latitude double precision,
        year integer,
        duration double precision,
        artist_id varchar,
        artist_longitude double precision,
        artist_location varchar
    )
""")

# Create dim tables

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id integer SORTKEY PRIMARY KEY,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar
    ) diststyle all;
""")


time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp SORTKEY PRIMARY KEY,
        hour integer,
        day integer,
        week integer,
        month integer,
        year integer,
        weekday integer
    ) diststyle all;
    
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar SORTKEY PRIMARY KEY,
        name varchar, 
        location varchar,
        latitude double precision,
        longitude double precision
    ) diststyle all;
    
""")


song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar SORTKEY DISTKEY PRIMARY KEY,
        title varchar,
        artist_id varchar REFERENCES artists (artist_id),
        year integer,
        duration double precision
    )
""")


# Create Fact Table
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id integer IDENTITY(0,1) PRIMARY KEY, 
        start_time timestamp REFERENCES time (start_time) SORTKEY,
        user_id integer REFERENCES users (user_id),
        level varchar,
        song_id varchar REFERENCES songs (song_id) DISTKEY,
        artist_id varchar REFERENCES artists (artist_id),
        session_id integer,
        location varchar,
        user_agent varchar
    )
""")


# ------ STAGING TABLES ------ 
# Queries to copy data from json files to staging tables

staging_events_copy = ("""
    COPY staging_events
    FROM {} 
    IAM_ROLE {}
    FORMAT AS JSON {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs
    FROM {} 
    IAM_ROLE {}
    FORMAT AS JSON {};
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['SONG_JSONOPTION'])


# ------ FINAL TABLES ------ 

# Queries to load from staging table to dim & fact tables

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, 
                        last_name, gender, level)
    SELECT DISTINCT userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE userId IS NOT NULL AND page = 'NextSong'

""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT(TIMESTAMP 'epoch' + ts * INTERVAL '1 Second') as start_time,
            EXTRACT(HOUR FROM start_time) as hour,
            EXTRACT(DAY FROM start_time) as day,
            EXTRACT(WEEK FROM start_time) as week,
            EXTRACT(MONTH FROM start_time) as month,
            EXTRACT(YEAR FROM start_time) as year,
            EXTRACT(WEEKDAY FROM start_time) as weekday
    FROM staging_events
    WHERE ts IS NOT NULL AND page = 'NextSong'
    
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id as artist_id, 
            artist_name as name,
            artist_location as location,
            artist_latitude as latitude, 
            artist_longitude as longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

songplay_table_insert = ("""
    INSERT INTO songplay(start_time, user_id, level,
                            song_id, artist_id, session_id,
                            location, user_agent)
    SELECT DISTINCT(TIMESTAMP 'epoch' + e.ts * INTERVAL '1 Second') as start_time,
            e.userId as user_id,
            e.level,
            s.song_id as song_id,
            s.artist_id as artist_id,
            e.sessionId as session_id,
            e.location as location,
            e.userAgent as user_agent
    FROM staging_events e
    JOIN staging_songs s
        ON s.artist_name = e.artist
        AND s.duration = e.length
        AND s.title = e.song
    WHERE start_time IS NOT NULL
        AND e.page = 'NextSong'
        AND song_id IS NOT NULL
        AND artist_id IS NOT NULL
        AND user_id IS NOT NULL
""")


# ------ QUERY LISTS ------ 

#Create tables
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, time_table_create, artist_table_create, song_table_create,  songplay_table_create]

#Drop tables
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

#Copy from S3 to staging tables
copy_table_queries = [staging_events_copy, staging_songs_copy]

#Load from staging table to dim and fact tables
insert_table_queries = [user_table_insert, time_table_insert, song_table_insert, artist_table_insert, songplay_table_insert,]
