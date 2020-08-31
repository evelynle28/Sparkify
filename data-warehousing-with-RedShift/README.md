# Sparkify: Data Warehouse
*Building data pipeline to move the music streaming app's data onto the cloud with RedShift*

![@(RedShift Logo)| center](https://d2adhoc2vrfpqj.cloudfront.net/2020/03/AmazonRedshift.png)

---
## Purpose and Goals

The purpose of this project is to move the data and process of Sparkify, a (hypothetical) start-up that wants to learn about their users' listening preferences on their music streaming app, onto the cloud. The data is currently residing in a S3 bucket. To makes the data accessible for the analytics team, this project will create a staging area along with a set of dimension and fact tables using a star schema in Redshift and build an ETL pipeline that transforms and loads data into the new database in RedShift using Python and SQL

---
## Database Schema Design
> Star Schema for Song Play Analysis

Using the song and log datasets, the database schema consists of one main fact table (*songplays*) containing all the measures associated to each event and referencing to 4 dimentional tables (*users*, *songs*, *artists*, and *time*)


#### Fact Table
1. **songplays** - *records in log data associated with song plays*
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables
1. **users** - *users in the app*
    - user_id, first_name, last_name, gender, level
2. **songs** - *songs in music database*
    - song_id, title, artist_id, year, duration
3. **artists** - *artists in music database*
    - artist_id, name, location, latitude, longitude
4. **time** - *timestamps of records in songplays broken down into specific units*
    - start_time, hour, day, week, month, year, weekday

---
## Dataset
> All dataset for the project resides in the **data** folder

### Song Dataset
Song dataset are json files residing in S3 at [@s3://udacity-dend/song_data](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend/song-data/?region=us-west-2&tab=overview). Each file contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track

A sample of a single song file:

```Python
{"song_id": "SOBLFFE12AF72AA5BA", "num_songs": 1, "title": "Scream", "artist_name": "Adelitas Way", "artist_latitude": null, "year": 2009, "duration": 213.9424, "artist_id": "ARJNIUY12298900C91", "artist_longitude": null, "artist_location": ""}
```

### Log Dataset
The second dataset consisting of log files in JSON format resides in S3 at [@s3://udacity-dend/log_data](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend/log-data/?region=us-west-2&tab=overview) These activity logs from a music streaming app based on specified configurations.

A sample of data in a log file:

```Python
{"artist":"Des'ree","auth":"Logged In","firstName":"Kaylee","gender":"F","itemInSession":1,"lastName":"Summers","length":246.30812,"level":"free","location":"Phoenix-Mesa-Scottsdale, AZ","method":"PUT","page":"NextSong","registration":1540344794796.0,"sessionId":139,"song":"You Gotta Be","status":200,"ts":1541106106796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.1916.153 Safari\/537.36\"","userId":"8"}
```

---
## ETL Pipeline

The ETL pipeline first extracts the data from the source database residing in AWS S3. Then, it creates 2 staging tables, ***staging_events*** and ***staging_songs***, in the RedShift cluster created. The pipeline then copy the user log data and the song metadata in to these 2 staging tables in JSON format. While staging the extracted data into RedShift, the pipeline also perform data cleasing, excluding certain invalid values, to preserve the accuracy and consistence of data in the target database (RedShift). The Fact and Dimension tables are created on RedShift based on the schema described above. After data is load into the staging area and all necessary tables are created, the ETL pipeline populates data from the staging tables into the dimension and fact tables created in RedShift.

---
## Run

The repo contains a config files that contains the AWS credentials and information related to your RedShift cluster and IAM role. Be mindful to fill out all the information before run the `etl.py`

```sh
$ git clone https://github.com/evelynle28/Sparkify.git
$ cd data-warehousing-with-RedShift
```

After filling out all information in the config file

```sh
$ python3 etl.py
```
