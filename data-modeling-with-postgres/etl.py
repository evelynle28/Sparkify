import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_files(cur, filepath):
    """
    Processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then, it extracts the artist information in order to store it into the artists table.

    INPUTS:
        * cur - the cursor variable
        * filepath - the file path to the song file
    """
    #open song_files
    df = pd.read_json(filepath, lines=True)

    #insert song records
    song_data = df[['song_id',
                    'title',
                    'artist_id',
                    'year',
                    'duration']]

    for i, record in song_data.iterrows():
        try:
            cur.execute(song_table_insert, list(record))
        except Exception as e:
            print("Failed song insertion:", e)

    #insert artist records
    artist_data = df[['artist_id',
                      'artist_name',
                      'artist_location',
                      'artist_latitude',
                      'artist_longitude']]

    for i, record in artist_data.iterrows():
        try:
            cur.execute(artist_table_insert, list(record))
        except Exception as e:
            print("Failed artist insertion:", e)

def process_log_files(cur, filepath):
    """
    Processes a log file whose filepath has been provided as an arugment.
    It extracts the user information and store it into the users table.
    Then, it extracts the timestamps of user's activity and store it into the time table.
    Along with information from dimensional tables constructed, it extracts the rest and
        load them into the songplays fact table.


    INPUTS:
        * cur - the cursor variable
        * filepath - the file path to the log file
    """

    df = pd.read_json(filepath, lines=True)

    #Filter records by `NextSong` action
    df = df[df['page'] == 'NextSong']

    #convert timestamp from ms units to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    #Break down timestamp into time units and insert into time table
    col_labels=['start_time',
                'hour',
                'day',
                'week',
                'month',
                'year',
                'weekday']

    time_data = [df['ts'].tolist(),
                  t.dt.hour.tolist(),
                  t.dt.day.tolist(),
                  t.dt.week.tolist(),
                  t.dt.month.tolist(),
                  t.dt.year.tolist(),
                  t.dt.weekday.tolist()]

    time_df = pd.DataFrame({col_labels[i]: time_data[i]
                          for i in range(len(col_labels))})

    #insert time records
    for i, record in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(record))
        except Exception as e:
            print("Failed time insertion: ", e)

    #insert users records:
    user_data = df[['userId',
                    'firstName',
                    'lastName',
                    'gender',
                    'level']]

    for i, record in user_data.iterrows():
        try:
            cur.execute(user_table_insert, list(record))
        except Exception as e:
            print("Failed user insertion: ", e)

    #insert songplays records
    for i, record in df.iterrows():
        #get song_id, artist_id from song and artist tables
        try:
            cur.execute(song_select,(record.song, record.artist, record.length))
        except Exception as e:
            print("Failed songplays insertion: ", e)
        else:
            results = cur.fetchone()
            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None

            #get songplay info
            songplay_data = [record.ts,
                              record.userId,
                              record.level,
                              songid,
                              artistid,
                              record.sessionId,
                              record.location,
                              record.userAgent]

            try:
                cur.execute(songplay_table_insert, songplay_data)
            except Exception as e:
                print("Failed songplays insertion: ", e)

def process_data(cur, conn, filepath, func):
    """
    Recursively walk the provided directory to get all needed json files
    Then, processes data based on its provided filepath

    INPUTS:
        * cur - the cursor variable
        * conn - connection to the database
        * filepath - the file path to the json files needed
        * func - processing function used to process files with the given filepath
    """

    #get all json files in the directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for file in files:
            all_files.append(os.path.abspath(file))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    #connect to sparkify database
    try:
        conn = psycopg2.connect(database="sparkify_db",
                                  user="evelyn",
                                  password="")
    except:
        print("Failed to connect to sparkify in etl\.py!")
    else:
        cur = conn.cursor()
        process_data(cur, conn, 'data/song_data', process_song_files)
        process_data(cur, conn, 'data/log_data', process_log_files)

    conn.close()


if __name__ == "__main__":
    main()
