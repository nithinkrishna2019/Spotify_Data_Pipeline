
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
from pyspark.sql.functions import explode,col,to_date
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame
s3_path = "s3://aws-glue-s3-bucket/spotify_Data_Pipeline/rawData/unprocessedData/"
source_dyf = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    connection_options={"paths":[s3_path]},
    format="json"
)
#converting dynamic frame to data frame

spotify_df = source_dyf.toDF()
def album_function(df):
    df_exploded=df.withColumn("items",explode("items"))
    df_album_final=df_exploded.select(
        col("items.track.album.id").alias("album_id"),
        col("items.track.album.name").alias("album_name"),
        col("items.track.album.release_date").alias("release_date"),
        col("items.track.album.total_tracks").alias("total_tracks"),
        col("items.track.album.external_urls.spotify").alias("url")
    ).drop_duplicates(["album_id"])
    
    return df_album_final
def artist_function(df):
    df_exploded=df.withColumn("items",explode("items"))
    
    df_artist_exploded=df_exploded.select(explode(col("items.track.artists")).alias("artist")) #we exploded again since artists is one more array 
    
    df_artist_final=df_artist_exploded.select(col("artist.id").alias("artist_id"),
                          col("artist.name").alias("artist_name"),
                          col("artist.external_urls.spotify").alias("artist_url")
                          ).drop_duplicates(["artist_id"])
    
    return df_artist_final
def songs_function(df):
    # Explode the items array to create a row for each song
    df_exploded = df.select(explode(col("items")).alias("item"))
    
    # Extract song information from the exploded DataFrame
    df_songs = df_exploded.select(
        col("item.track.id").alias("song_id"),
        col("item.track.name").alias("song_name"),
        col("item.track.duration_ms").alias("duration_ms"),
        col("item.track.external_urls.spotify").alias("url"),
        col("item.track.popularity").alias("popularity"),
        col("item.added_at").alias("song_added"),
        col("item.track.album.id").alias("album_id"),
        col("item.track.artists")[0]["id"].alias("artist_id")
    ).drop_duplicates(["song_id"])
    
    # Convert string dates in 'song_added' to actual date types
    df_songs_final = df_songs.withColumn("song_added", to_date(col("song_added")))
    
    return df_songs_final
album_df = album_function(spotify_df)
artist_df = artist_function(spotify_df)
song_df = songs_function(spotify_df)
def write_to_s3(df, path_suffix, format_type="csv"):
    # Convert back to DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    
    glueContext.write_dynamic_frame.from_options(
        frame = dynamic_frame,
        connection_type = "s3",
        
        connection_options = {"path": f"s3://aws-glue-s3-bucket/spotify_Data_Pipeline/transformedData/{path_suffix}/"},
        format = format_type
    )
#write data to s3   
write_to_s3(album_df, "album_Data/album_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")
write_to_s3(artist_df, "artist_Data/artist_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")
write_to_s3(song_df, "songs_Data/songs_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")

job.commit()