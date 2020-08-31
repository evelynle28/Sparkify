# Sparkify: Data Modeling
*Data Modeling with Apache Cassandra*

---
## Goals and Purposes

The analysis team of Sparkify, a (hypothetical) music streaming app start-up, is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

This project will build an Apache Cassandra database which can create queries on song play data to answer the questions. The result database can be tested by by running queries given in the notebook

---
## Project Files

**Data_Modeling_With_Apache_Cassandra.ipynb**: This notebook contains the ETL pipeline.

**event-data**: This folder contains original, normalized data containing events in the music streaming app in csv form.

---
## ETL pipeline

The first part of the notebook denormalize the original data the `event-data` folder. The preprocessed data will be written into a new file. Then, in the second part, the notebook creates the Cluster and Keyspace. The ETL pipelinne also creates the tables needed for the given queries based on the schema attached in the notebook. Finally, it will populate the data into the created tables. Queries are included at the end of the notebook to test if the data was properly modelled.
