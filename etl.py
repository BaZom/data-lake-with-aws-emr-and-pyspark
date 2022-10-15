import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ creates a spark session
           
    Args:
        no Args
        
    Returns:
        returns an object of the sparkSession
    """    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ reads song data from files in an input source into dataframes, which are saved as parquet files in an output destination  
           
    Args:
        spark (obj): object of SparkSession
        input_data (str): path of the source data
        output_data (str): path of the output destintaion 
        
    Returns:
        no return values
    """ 
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/'
    
    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).filter(col("song_id").isNotNull()).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').parquet(output_data + "song-table-data/songs.parquet", partitionBy=['year','artist_id'])
    
    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]).filter(col("artist_id").isNotNull()).distinct()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artist-table-data/artists.parquet", partitionBy=['artist_id'])

def process_log_data(spark, input_data, output_data):
    """ reads log data from files in an input source into dataframes, which are saved as parquet files in an output destination  
           
    Args:
        spark (obj): object of SparkSession
        input_data (str): path of the source data
        output_data (str): path of the output destintaion 
        
    Returns:
        no return values
    """     
    # get filepath to log data file
    log_data = input_data + 'log_data'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter('page = "NextSong"')
    
    # extract columns for users table     
    users_table = df.select([col("userId").alias("user_id"),\
                             col("firstName").alias("first_name"),\
                             col("lastName").alias("last_name"),\
                             "gender", "level"]).filter(col("userId").isNotNull()).distinct()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "user-table-data/users.parquet", partitionBy=['user_id'])

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    df = df.withColumn('timestamp', get_timestamp(col('ts')))    

    # extract columns to create time table
    time_table = df.select("timestamp").alias("start_time")\
                            .withColumn("hour", hour(col('timestamp')))\
                            .withColumn("day", dayofmonth(col('timestamp')))\
                            .withColumn("week", weekofyear(col('timestamp')))\
                            .withColumn("month", month(col('timestamp')))\
                            .withColumn("year", year(col('timestamp')))\
                            .withColumn("weekday", dayofweek(col('timestamp')))\
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').parquet(output_data + "time-table-data/time.parquet", partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/')
    
    # extract columns from joined song and log datasets to create songplays table 
    cond = [song_df.title == df.song, song_df.artist_name == df.artist]
    songplays_table = df.join(song_df, cond, "left")
    songplays_table = songplays_table.select([monotonically_increasing_id().alias("songplay_id"),\
                                              col("timestamp").alias("start_time"),\
                                              col("userId").alias("user_id"),\
                                              "level",\
                                              "song_id",\
                                              "artist_id",\
                                              col("sessionId").alias("session_id"),\
                                              "location",\
                                              col("userAgent").alias("user_agent"),\
                                              year(col("timestamp")).alias("year"),\
                                              month(col("timestamp")).alias("month")]).filter(col("song").isNotNull()).distinct()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').parquet(output_data + "songplays-table-data/songplays.parquet", partitionBy=['year', 'month'])
    songplays_table.show()

def main():
    """ calls create_spark_session, prcess_song_data and process_log_data. Also defines the paths of the input data and output destination
           
    Args:
        No Args
        
    Returns:
        no return values
    """     
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
