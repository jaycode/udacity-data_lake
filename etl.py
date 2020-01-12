import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.window import Window


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process song data from given input_path and store it at output_data as parquet files.
    
    Args:
        spark(pyspark.sql.SparkSession): SparkSession object
        input_data(string): Path to input data directory. End with '/' e.g. 'data/log/'
        output_data(string): Path to output data directory. End with '/' e.g. 'data/log/'
    Returns:
        None
    """
    # get filepath to song data file
    song_data = input_data
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("staging_songs")
    songs_table = spark.sql("""
        SELECT song_id, title, artist_id, year, duration
        FROM staging_songs
        ORDER BY song_id
    """)

    # write songs table to parquet files partitioned by year and artist
    songs_table_path = "{}{}".format(output_data, 'songs.parquet')
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(songs_table_path)

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT artist_id, artist_name AS name, artist_location AS location,
               artist_latitude AS latitude, artist_longitude AS longitude
        FROM staging_songs
        ORDER BY artist_id
    """)

    # write artists table to parquet files
    artists_table_path = "{}{}".format(output_data, 'artists.parquet')
    artists_table.write.mode('overwrite').parquet(artists_table_path)


def process_log_data(spark, input_data, output_data):
    """Process song data from given input_path and store it at output_data as parquet files.
    
    Args:
        spark(pyspark.sql.SparkSession): SparkSession object
        input_data(string): Path to input data directory. End with '/' e.g. 'data/log/'
        output_data(string): Path to output data directory. End with '/' e.g. 'data/log/'
    Returns:
        None
    """
    
    # get filepath to log data file
    log_data = input_data

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df.createOrReplaceTempView("staging_events")
    df = spark.sql("""SELECT * FROM staging_events WHERE page='NextSong'""")

    # extract columns for users table    
    users_table = spark.sql("""
        SELECT DISTINCT userId AS user_id, firstName AS first_name, lastName as last_name,
                        gender, level
        FROM staging_events
        ORDER BY user_id
    """)

    # write users table to parquet files
    users_table_path = "{}{}".format(output_data, 'users.parquet')
    users_table.write.mode('overwrite').parquet(users_table_path)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0))
    df = df.withColumn('timestamp', get_timestamp('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('datetime', get_datetime('ts'))
    df.createOrReplaceTempView("staging_events")

    # extract columns to create time table
    time_table = spark.sql("""
        SELECT  DISTINCT datetime AS start_time, hour(timestamp) AS hour, day(timestamp)  AS day,
                         weekofyear(timestamp) AS week, month(timestamp) AS month, year(timestamp) AS year,
                         dayofweek(timestamp) AS weekday
        FROM staging_events
        ORDER BY start_time
    """)

    # write time table to parquet files partitioned by year and month
    time_table_path = "{}{}".format(output_data, 'time.parquet')
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(time_table_path)
    
    # read in song data to use for songplays table
    songs_table = spark.read.parquet("{}{}".format(output_data, 'songs.parquet'))
    artists_table = spark.read.parquet("{}{}".format(output_data, 'artists.parquet'))
    w = Window().orderBy('song_id')


    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(songs_table.alias("s"), df.song == col('s.title')) \
                        .join(artists_table.alias("a"), df.artist == col('a.name')) \
                        .select(
                            col('timestamp').alias('start_time'),
                            col('userId').alias('user_id'),
                            'level',
                            's.song_id',
                            'a.artist_id',
                            col('sessionId').alias('session_id'),
                            'a.location',
                            col('userAgent').alias('user_agent')) \
                        .withColumn('songplay_id', row_number().over(w))

    # write songplays table to parquet files
    songplays_table_path = "{}{}".format(output_data, 'songplays.parquet')
    songplays_table.write.mode('overwrite').parquet(songplays_table_path)


def main():
    """Main function of our application."""
    global config
    
    spark = create_spark_session()
    
    output_data = config[config['DEFAULT']['DATA_LOCATION']]['OUTPUT_DATA']
    song_data = config[config['DEFAULT']['DATA_LOCATION']]['SONG_DATA']
    log_data = config[config['DEFAULT']['DATA_LOCATION']]['LOG_DATA']
    
    process_song_data(spark, song_data, output_data)    
    process_log_data(spark, log_data, output_data)


if __name__ == "__main__":
    main()
