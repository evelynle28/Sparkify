# Sparkify: Database & ETL Pipeline
*Data Modeling with Postgres for music streaming app*

![@| center | 500x0](https://miro.medium.com/max/1200/1*7AOhGDnRL2eyJMUidCHZEA.jpeg)

---
## Purpose and Goals

The purpose of this project is to create a Postgres database that optimizes queries on song play analysis and build an ETL pipeline to transfer data from the Sparkify's datasets. Sparkify is a (hypothetical) start-up that wants to learn about their users' listening preferences on their music streaming app. To make the collected data more accessible for Sparkify's analytics team, this project creates a database using a star schema and builds an ETL pipeline to transfer data in JSON format from 2 local directories into the tables using Python and SQL.

---
## Database Schema Design
> Star Schema for Song Play Analysis

Using the song and log datasets, the database schema consists of one main fact table (*songplays*) containing all the measures associated to each event and referencing to 4 dimentional tables (*users*, *songs*, *artists*, and *time*)

![@Sparkify Database Schema | center | 500x0](https://raw.githubusercontent.com/evelynle28/Sparkify/master/data-modeling-with-postgres/img/Database%20Schema.png)

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
Song dataset are json files nested in subdirectories under */data/song_data*. Each file contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track

A sample of a single song file:

```Python
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

### Log Dataset
The second dataset consisting of log files in JSON format are in subdirectories under */data/log_data*. These activity logs from a music streaming app based on specified configurations.

A sample of data in a log file:
```Python
{"artist":"Mitch Ryder & The Detroit Wheels","auth":"Logged In","firstName":"Tegan","gender":"F","itemInSession":65,"lastName":"Levine","length":205.03465,"level":"paid","location":"Portland-South Portland, ME","method":"PUT","page":"NextSong","registration":1540794356796.0,"sessionId":992,"song":"Jenny Take A Ride (LP Version)","status":200,"ts":1543363215796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"80"}
```

---
## ETL Pipeline

The ETL Pipeline first reads and processes files from ***song_data***, the first dataset. It creates ***songs*** and ***artists*** dimensional tables, extracts data from JSON files in the dataset, and loads them as records into the Postgres database. Then, the pipeline reads and processes data from the second dataset, ***log_data***. Using the data extracted from this dataset, the ETL pipeline creates ***time*** and ***users*** dimensional tables, as well as the ***songplays*** fact table, and transfer all the data into the Postgres database.
