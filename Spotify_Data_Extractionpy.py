import json
import spotipy
from spotipy.oauth2 import SpotifyOAuth,SpotifyClientCredentials
import os
import boto3
from datetime import datetime

def lambda_handler(event, context):

    client_id=os.environ.get('client_id')
    client_secret=os.environ.get('client_secret')
    link=os.environ.get('link')

    client_cred_manager=SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
    sp=spotipy.Spotify(client_credentials_manager=client_cred_manager)


    Auro_10000_playlist_link=link

    list_split=Auro_10000_playlist_link.split('/')
    play_list_uri=list_split[-1]

    spotify_data=sp.playlist_tracks(play_list_uri)

    print(spotify_data)
    
    filename="spotify_raw_"+str(datetime.now())+".json"

    bucket_name = "aws-glue-s3-bucket"
    source_prefix = f"spotify_Data_Pipeline/rawData/unprocessedData/{filename}"

    client=boto3.client('s3')
    
    client.put_object(
        Bucket=bucket_name,
        Key=source_prefix,
        Body=json.dumps(spotify_data)
        
    )




