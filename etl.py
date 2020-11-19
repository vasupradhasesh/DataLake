import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')


#os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

os.environ['AWS_ACCESS_KEY_ID']=""
os.environ['AWS_SECRET_ACCESS_KEY']=""


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
"""
Description: This function can be used to read the file in the filepath (data/song_data)
to get the artist and song info and used to populate the artist and song tables.

Returns:
    None
"""
    # get filepath to song data file
    song_data = "{}song_data/*/*/*/*.json".format(input_data)
    
    # read song data file
    df = spark.read.format("json").load(song_data)
    print(df)
    print('Song Data file read!!')
    # year integer type conversion
    df = df.withColumn("year", df["year"].cast(IntegerType()))

    
    # extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year','artist_id']).parquet("{}/songs.parquet".format(output_data), mode="overwrite")

    # extract columns to create artists table
    artists_table = spark.sql("""
                                SELECT DISTINCT
                                       artist_id,
                                       artist_name as name,
                                       artist_location as location,
                                       artist_latitude as latitude,
                                       artist_longitude as longitude
                                FROM songs
                             """)
    
    # write artists table to parquet files
    artists_tablewrite.parquet("{}/artists.parquet".format(output_data), mode="overwrite")


def process_log_data(spark, input_data, output_data):
"""
Description: This function can be used to read the file in the filepath (data/log_data)
to get the user and time info and used to populate the users and time dim tables.
Join the tables and create songplay table.

Returns:
    None
"""
    # get filepath to log data file
    log_data = "{}log_data/*/*/*.json".format(input_data)

    # read log data file
    df = spark.read.format("json").load(log_data)
    print(df)
    print('Log Data file read!!')
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr('userId as user_id','firstName as first_name','lastName as last_name','gender','level').distinct()
    
    # write users table to parquet files
    users_table.write.parquet("{}/users.parquet".format(output_data), mode="overwrite")
   

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)))
    df = df.withColumn("start_time", get_datetime("ts"))
    
    # extract columns to create time table
    time_table = (
        df
        .withColumn("hour", hour("start_time"))
        .withColumn("day", dayofmonth("start_time"))
        .withColumn("week", weekofyear("start_time"))
        .withColumn("month", month("start_time"))
        .withColumn("year", year("start_time"))
        .withColumn("weekday", dayofweek("start_time"))
        .select("start_time", "hour", "day", "week", "month", "year", "weekday")
        .distinct()
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time_table")

    # read in song data to use for songplays table
    song_df = spark.read.parquet("{}/songs.parquet".format(output_data))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT ld.start_time,
                                       tt.year,
                                       tt.month,
                                       ld.userid,
                                       ld.level,
                                       qq.song_id,
                                       qq.artist_id,
                                       ld.sessionid,
                                       ld.location,
                                       ld.useragent
                                  FROM log_data ld
                                  JOIN time_table tt ON (ld.start_time = tt.start_time)
                                 LEFT JOIN (
                                           SELECT st.song_id,
                                                  st.title,
                                                  art.artist_id,
                                                  art.artist_name
                                             FROM songs_table st
                                             JOIN artists_table art ON (st.artist_id = art.artist_id)
                                          ) AS qq ON (ld.song = qq.title AND ld.artist = qq.artist_name)
                               """)


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + "songplays_table")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
