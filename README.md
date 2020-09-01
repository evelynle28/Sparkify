# Sparkify: Data Engineering projects

![(@Data Engineer)| center](https://mk0analyticsindf35n9.kinstacdn.com/wp-content/uploads/2017/11/why-data-engineer-trending-01.jpg)

---
## Overview

Sparkify is a (hypothetical) music streaming start-up that wants to learn about their users' listening preferences on their app. To make the collected data more accessible for Sparkify's analytics team, these projects will design and build database system based on the needs and scale of the company at different time. The first two projects will perform data modeling with Progres and Cassandra. The third will utilize Amazon RedShift and AWS S3 build a data warehouse on Cloud. As the company continues to grow, the fourth project will build a data lake with Spark, AWS EMR, and AWS S3.The last project in this repo will orchestrate and automate a data workflow using Apache Airflow.

---
## Project 1: Data Modeling with Postgres

The purpose of this project is to create a Postgres database that optimizes queries on song play analysis and build an ETL pipeline to transfer data from the Sparkify's datasets. To make the collected data more accessible for Sparkify's analytics team, this project creates a database using a star schema and builds an ETL pipeline to transfer data in JSON format from 2 local directories into the tables using Python and SQL.

Details about project 1: [Data Modeling with Progres](https://github.com/evelynle28/Sparkify/tree/master/data-modeling-with-postgres)

---
## Project 2: Data Modeling with Cassandra

The analysis team of Sparkify is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app. This project will build an Apache Cassandra database around the queries that we need to answer the analysis team's questions. The result database can be tested by by running queries given in the noteboo

Details about project 2: [Data Modeling with Cassandra](https://github.com/evelynle28/Sparkify/tree/master/data-modeling-with-apache-cassandra)

---
## Project 3: Data Warehouse
The purpose of this project is to move the data and process of Sparkify onto the cloud. The data is currently residing in a S3 bucket. To makes the data accessible for the analytics team, this project will create a staging area along with a set of dimension and fact tables using a star schema in Redshift and build an ETL pipeline that transforms and loads data into the new database in RedShift using Python and SQL

Details about project 3: [Data Warehousing with RedShift](https://github.com/evelynle28/Sparkify/tree/master/data-warehousing-with-RedShift)

---
## Project 4: Data Lake
This project will build an ETL pipeline for a data lake hosted on AWS S3. As Sparkify's users and song database scaled rapidly, the company needs to move their data to a data lake. Their data is currently storing in S3 in JSON format, with a directory of users' activity logs and another on of their songs' metadata. This project will extract data from the S3 storage, use Spark to process it, and loads back to an S3 storage as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

Details about project 4: [Data Lake with Spark and EMR](https://github.com/evelynle28/Sparkify/tree/master/data-lake-with-spark)


