import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear,dayofweek, date_format
from pyspark.sql.types import * 
import pandas as pd
from zipfile import ZipFile

def unzip_files(input_data, zip_path, unzip_dir):
    """ Checks for existance of zip files and unzip directory, 
    unzips files stored in the local directory and prints out steps taken
    if successful
    Arg:
    input_data -- local path to zipped files
    unzip_dir -- local path to store unzipped data
    """
    if (not os.path.isfile(input_data + zip_path)) and (not os.path.isdir(input_data + unzip_dir)):
        with ZipFile(input_data + zip_path, 'r') as zip_ref:
            zip_ref.extractall(input_data + unzip_dir)
        print('Zip file has been extracted.')
    else:
        print('Zip file and dir already exist.')

def create_spark_session():
    """Creates spark session and returns it"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Processes song data by reading from input_data (local directory), 
    creating song_table and artist table with select columns, 
    dropping duplicates and writing them back to output location in parquet file.
    
    Arg:
    spark -- spark session
    input_data -- original source of data to consume
    output_data -- output location to write the processed data     
    """
    # get filepath to song data file
    song_data = input_data + 'local-song-data/song_data/*/*/*/*.json' 
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + 'songs/', mode='overwrite', partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/', mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """ Processes log data by reading from input_data, 
    creating song_table and artist table with select columns, 
    dropping duplicates, creating additional derived fields (time_table) 
    and writing them back to output  location in parquet file.
    
    Arg:
    spark -- spark session
    input_data -- original source of data to consume
    output_data -- putput location to write the processed data     
    """
    # get filepath to log data file
    log_data = input_data + 'local-log-data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').drop_duplicates() 
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users', mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.utcfromtimestamp(int(x) / 1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts')) 
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.utcfromtimestamp(int(x) / 1000), DateType())
    df = df.withColumn('start_time_dtf', get_datetime('ts'))
    
    # extract columns to create time table
    time_table = df.withColumn('hour', hour('start_time'))\
        .withColumn('day', dayofmonth('start_time'))\
        .withColumn('week', weekofyear('start_time'))\
        .withColumn('month', month('start_time'))\
        .withColumn('year', year('start_time'))\
        .withColumn('weekday', dayofweek('start_time'))\
        .select('ts', 'start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday').dropDuplicates() 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'time', mode='overwrite', partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    song_df = spark.read.format('json').load(input_data + 'local-song-data/song_data/*/*/*').drop_duplicates()

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.song == song_df.title, how='inner') \
        .select(monotonically_increasing_id().alias('songplay_id'), col('start_time'),col('userId').alias('user_id') \
        , 'level', 'song_id', 'artist_id', col('sessionId').alias('session_id'), 'location', col('userAgent').alias('user_agent'))
    
    # add year and month columns to use to partition by in the next step
    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, how='inner')\
        .select('songplay_id', songplays_table.start_time, 'user_id', 'level', 'song_id', 'artist_id' \
        , 'session_id', 'location', 'user_agent', 'year', 'month').drop_duplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + 'songplays', mode='overwrite', partitionBy=['year', 'month'])


def main():
    """ Runs steps to execute the etl process """
    spark = create_spark_session()

    input_data = 'data/'
    output_data = 'data/output-data/'
    
    unzip_files(input_data, 'song-data.zip', 'local-song-data/')
    unzip_files(input_data, 'log-data.zip', 'local-log-data/')
        
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
