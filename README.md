# Udacity Data Engineer Nanodegree Data Lake Project

Submission for Udacity DEND Nanodegree Data Lake project.

- [Link to rubric](https://review.udacity.com/#!/rubrics/2502/view)
- [Link to project instructions](https://classroom.udacity.com/nanodegrees/nd027/parts/19ef4e55-151f-4510-8b5c-cb590ac52df2/modules/743a0ff8-d0ad-4a63-a459-46601f8a4446/lessons/cfdfde06-9c87-4c72-aa09-2aee8eb35675/concepts/last-viewed?contentVersion=2.0.0&contentLocale=en-us)

**Note:** Please rename the file `dl.default.cfg` to `dl.cfg` and adjust appropriately before running `etl.py` script. To use AWS S3 path, replace `DATA_LOCATION=LOCAL` with `DATA_LOCATION=AWS`

## Introduction
The purpose of the database is to make it easier for the data science team at Sparkify to analyze their app's users' behaviors. Searching for particular song play event or a group of them can be done by running SQL queries to the final database.

## Schema Design

![schema](schema.png)

List of tables
- staging_events: Staging table for data from `s3://udacity-dend/log_data`
- staging_songs: Staging table for data from `s3://udacity-dend/song_data`
- songplays: Log data of song plays.
- users: Users in the app.
- songs: Songs in the app's music database.
- artists: Artists in the app's music database.
- time: Timestamps of records in songplays.

## How to run the scripts: 

```
$ python etl.py
```

## Explanation of the files in the repository:

1. `dwh.default.cfg`: Configurations for running all the script files. Rename to `dwh.cfg` before running the scripts.
2. `etl.py`: Use Apache Spark to pull song data and log data from an S3 or local path, and then build and load analytical tables from them.
5. `README.md`: This document.
6. `test.ipynb`: A Jupyter notebook document I used during the development of this project to give direct feedback as I wrote the code.

## ETL Pipeline

1. Copy all data from `s3://udacity-dend/song_data` and `s3://udacity-dend/log_data` to temporary views `staging_songs` and `staging_events`, respectively. They are not committed into the system's memory, instead functioning like hive tables. This is achieved through Spark's function `createOrReplaceTempView()`. These staging tables have the exact same structure with the raw json files.
2. Run SQL queries to select data from temporary views to load the other 5 tables (i.e. our OLAP/analytical tables). Duplicate rows are handled by `SELECT DISTINCT`.
3. For each table, store its parquet version directly after loading the data.