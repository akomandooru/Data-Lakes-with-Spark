import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
import shutil
import time

# setup configuration
config = configparser.ConfigParser()
# read configuration from dl.cfg file
config.read('dl.cfg')
# load environment with the keys
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

# Create a spark session
def create_spark_session():
    """
        This function creates a spark session
        Output: new or existing spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

# Process song data
def process_song_data(spark, input_data, output_data):
    """
        This function will ETL song data
        Inputs: spark session, input data location, output data location        
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
        
    # read song data file
    df = spark.read.json(song_data)
    #create a temporary view for query later on
    df.createOrReplaceTempView("song_data")

    # extract columns to create songs table    
    songs_table = spark.sql("""SELECT song_id, title, artist_id, year, duration FROM song_data""")
    
    # write songs table to parquet files partitioned by year and artist
    shutil.rmtree(output_data + "songs_table", ignore_errors=True)
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs_table")

    # extract columns to create artists table
    artists_table = spark.sql("""SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM song_data""")
    
    # write artists table to parquet files; delete pre-existing files if they exist
    shutil.rmtree(output_data + "artists_table", ignore_errors=True)
    artists_table.write.parquet(output_data + "artists_table")

# Process log data
def process_log_data(spark, input_data, output_data):
    """
        This function will ETL log data
        Inputs: spark session, input data location, output data location
    """
    # get filepath to log data file
    log_data = input_data  + "log_data/*/*/*.json"
    
    # read log data file
    df = spark.read.json(log_data)
    # create a temporary view for use in a query later on
    df.createOrReplaceTempView("log_data")
    
    # filter by actions for song plays
    df = spark.sql("""SELECT * FROM log_data WHERE page='NextSong'""")

    # extract columns for users table    
    users_table = spark.sql("""
        SELECT userId as user_id, min(firstName) as first_name, min(lastName) as last_name, min(gender) as gender, min(level) as level
        FROM log_data GROUP BY userId""")
    
    # write users table to parquet files; delete pre-existing files if they exist
    shutil.rmtree(output_data + "users_table",ignore_errors=True)
    users_table.write.parquet(output_data + "users_table")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000)
    df = df.withColumn('timestamp', get_timestamp(df['ts']))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: time.localtime(x) )
    df = df.withColumn('datetime', get_datetime(df['timestamp']))
    
    # replace the log_data view to update for the two new columns we added for timestamp and date
    df.createOrReplaceTempView("log_data")
    
    # extract columns to create time table
    time_table = spark.sql("""SELECT ts as start_time, hour(timestamp) as hour, dayofmonth(timestamp) as day, weekofyear(timestamp) as week, month(timestamp) as month, year(timestamp) as year, dayofweek(timestamp) as weekday FROM log_data""")
    
    # write time table to parquet files partitioned by year and month; delete pre-existing files if they exist
    shutil.rmtree(output_data + "time_table",ignore_errors=True)
    time_table.write.partitionBy("year","month").parquet(output_data + "time_table")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs_table")
    # create a temp view for use in a query later on
    song_df.createOrReplaceTempView("song_view")
    # read in artists data to use for songplays table
    artist_df = spark.read.parquet(output_data + "artists_table")
    # create a temp view for use in a query later on
    artist_df.createOrReplaceTempView("artist_view")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
            SELECT monotonically_increasing_id() as songplay_id, ts as start_time, userId as user_id, level, sv.song_id, av.artist_id, 
            sessionId as session_id,
            location, userAgent as user_agent FROM log_data ld
            LEFT JOIN song_view sv ON ld.song=sv.title
            LEFT JOIN artist_view av ON ld.artist=av.artist_name""")

    # write songplays table to parquet files partitioned by user; delete pre-existing files if they exist
    shutil.rmtree(output_data + "songplays_table",ignore_errors=True)
    songplays_table.write.partitionBy("user_id").parquet(output_data + "songplays_table")


def main():
    """
        This is the main function for this ETL process; it will
        setup a spark session, ETL input files residing on s3a 
        to a local output folder (output folder can be setup to be on s3 too)
    """
    # create a spark session
    spark = create_spark_session()
    # setup input file path - s3a://udacity-dend in this case
    input_data = "s3a://udacity-dend/"    
    # setup output file path - local data/output in this case; can be changed to an s3 bucket 
    output_data = "./data/output/"
    
    # process (ETL) song data
    process_song_data(spark, input_data, output_data)    
    # process (ETL) log data
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
