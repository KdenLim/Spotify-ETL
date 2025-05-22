import pandas as pd 
from datetime import timedelta
from datetime import datetime
from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook
import requests
from airflow.models import Variable
from tabulate import tabulate
import pytz
import base64

# Setting up variables
conn = BaseHook.get_connection("spotify_api")
client_id = conn.login
client_secret = conn.password
redirect_uri = Variable.get("redirect_uri")
auth_code = Variable.get("auth_code")
EST = pytz.timezone("Asia/Kuala_Lumpur") 
my_db = 'song_mysql' # The name of your DB which you have saved under connection in Airflow UI


def _exchange_token():
    """
    This function is responsible for requesting tokens and setting up the access token and refresh token received into variables in Airflow
    """
    auth_url = 'https://accounts.spotify.com/api/token'
    auth_payload = {
        'grant_type': 'authorization_code',
        'code': auth_code,
        'redirect_uri': redirect_uri,
        'client_id': client_id,
        'client_secret': client_secret
    }

    response = requests.post(auth_url, data=auth_payload)

    if response.status_code == 200:
        tokens = response.json()
        Variable.set("spotify_access_token", tokens["access_token"])
        Variable.set("spotify_refresh_token", tokens["refresh_token"])
        print("Tokens are succesfully set")
    else:
        print("Failed to obtain tokens:", response.status_code, response.text)
        
def _refresh_token():
    """
    This function is responsible for refreshing the access token and replacing the variable in Airflow
    """
    refresh_token = Variable.get("spotify_refresh_token")
    auth_url = 'https://accounts.spotify.com/api/token'
    auth_payload = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': client_id,
        'client_secret': client_secret
    }

    response = requests.post(auth_url, data=auth_payload)

    if response.status_code == 200:
        access_token = response.json().get('access_token')
        Variable.set("spotify_access_token", access_token)
        print('New access token obtained')
    else:
        print(f'Failed to refresh access token. Response: {response.json()}')

def _get_recently_played_tracks():
    """
    This function retrieves tracks played in the last 24 hours and adds them into the database 
    """
    access_token = Variable.get("spotify_access_token")
    headers = {
        'Authorization': f'Bearer {access_token}',
    }
    yesterday = datetime.now(EST)-timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
    recently_played_url = f'https://api.spotify.com/v1/me/player/recently-played?limit=50&after={yesterday_unix_timestamp}'
    print(recently_played_url)
    response = requests.get(recently_played_url, headers=headers)

    if response.status_code == 200:
        recently_played_tracks = response.json()
    else:
        print(f'Failed to get recently played tracks. Response: {response.json()}')

    data = recently_played_tracks

    # Initialise lists to store data on recently played tracks
    song_names, album_names, artist_names, played_at_list, timestamps= [], [], [], [], []

    # Extracting only the relevant bits of data from the json object      
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        album_names.append(song["track"]["album"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
        
    # Prepare a dictionary in order to turn it into a pandas dataframe below       
    song_dict = {
        "song_name" : song_names,
        "album_name" : album_names,
        "artist_name": artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name","album_name", "artist_name", "played_at", "timestamp"])
    print(tabulate(song_df))
    print(song_df)

    sql_query = """
    CREATE TABLE IF NOT EXISTS recently_played_tracks(
        song_name VARCHAR(255),
        album_name VARCHAR(255),
        artist_name VARCHAR(255),
        played_at VARCHAR(255),
        timestamp VARCHAR(255),
        PRIMARY KEY (played_at)
    )
    """

    #pulling variables from a mysql connection with the help of MySqlHook
    mysql_connection = MySqlHook.get_connection(my_db)  
    con_str = 'mysql://'+ str(mysql_connection.login)+':'+str(mysql_connection.password) + '@' + str(mysql_connection.host) +':'+str(mysql_connection.port)+'/'+str(mysql_connection.schema)
    engine = create_engine(con_str,echo=True) 

    # Creating a table if it doesn't exist
    try:
        engine.execute(sql_query)
        print("Table created or already exists")
    except Exception as e:
        print("Error creating table: ", e)

    # Loading song dataframe into the database   
    try:
        song_df.to_sql("recently_played_tracks", engine, index=False, if_exists='append', method='multi')
        print("Data inserted succesfully")
    except Exception as e:
        print("Error inserting data: ", e)

    engine.dispose() # Closing the engine 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
}

with DAG(
    'spotify_dag',
    default_args=default_args,
    schedule=timedelta(days=1),  
):

    exchange = PythonOperator(
        task_id = 'exchange_token',
        python_callable = _exchange_token
    )

    refresh = PythonOperator(
        task_id = 'refresh_token',
        python_callable = _refresh_token
    )

    get_songs = PythonOperator(
        task_id= 'get_songs',
        python_callable = _get_recently_played_tracks
    )

# Dependancies 
exchange >> refresh >> get_songs