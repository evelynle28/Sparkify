# Sparkify: Data Lake
Building Data Lake & ETL pipeline with Spark, AWS S3, and AWS EMR

![(@Spark logo)| center](https://www.inovex.de/blog/wp-content/uploads/2016/12/spark.png)

---
## Project Goals

The purpose of the is project is to build an ETL pipeline for a data lake hosted on AWS S3. Sparikfy is a growing music streaming startup. As their users and song database scaled rapidly, the company needs to move their data to a data lake. Their data is currently storing in S3 in JSON format, with a directory of users' activity logs and another on of their songs' metadata. This project will extract data from the S3 storage, use Spark to process it, and loads back to an S3 storage as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

---
## Database Schema Design
*Star Schema for Song Play Analysis*


Using the song and log datasets, the database schema consists of one main fact table (*songplays*) containing all the measures associated to each event and referencing to 4 dimentional tables (*users*, *songs*, *artists*, and *time*)

![(@Sparkify Database Schema) | center](https://raw.githubusercontent.com/evelynle28/Sparkify/master/data-modeling-with-postgres/img/Database%20Schema.png)

### Fact Table
1. **songplays** - *records in log data associated with song plays*
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
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
*All dataset for the project resides in the `data` folder*

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

The ETL Pipeline first extract the data currently residing in an S3 storage. Then, using Spark, the pipeline will create 5 separated tables, including 1 fact table and 4 dimensional ones, as specified in the schema above. Then the pipeline will populate data into these tables and write them into parquet files, partitioning based on data of each table. These analytics tables then will be loaded and stored in a data lake hosted on AWS S3. The Spark process will be deployed on a cluster using AWS.

---
## Run locally

Clone this repo

```sh
$ git clone https://github.com/evelynle28/Sparkify.git
$ cd data-warehousing-with-RedShift

```
The repo contains a config files that contains the AWS credentials. Be mindful to fill out all the information before run the etl.py. At the same time, your destination bucket is also needed to be specified in `etl.py`

To run the elt pipeline locally

```sh
$ python3 etl.py
```

---
## Run the pipeline on an EMR
You can create an EMR cluster on AWS and submit this script as well.

Follow [this instruction](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs-launch-sample-cluster.html) to create an EMR cluster and establish a SSH access to the cluster. Choose the configuration included Spark when you launch the EMR cluster.

On your terminal, clone the repo and submit the `elt.py`

```sh
$ /usr/bin/spark-submit --master yarn ~/data-lake-with-spark/etl.py
```

The whole process should take around 30 minutes with 3 nodes.


