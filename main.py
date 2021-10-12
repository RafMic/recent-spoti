import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery

CLIENT_ID = '<YOUR_CLIENT_ID>'
SECRET_ID = '<YOUR_SECRET_ID>'


def is_valid(df: pd.DataFrame) -> bool:
    if df.empty:
        raise Exception("Data frame is empty")

    if not df['played_at_UTC'].is_unique:
        raise Exception("PK violation")

    if df.isnull().values.any():
        raise Exception("Data frame has null values")

    return True


def get_data(data, context):
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=CLIENT_ID,
                                                   client_secret=SECRET_ID,
                                                   scope="user-read-recently-played",
                                                   redirect_uri="http://localhost"))

    hour_ago = int((datetime.now(timezone.utc) -
                    timedelta(hours=1)).timestamp() * 1000)
    recent = sp.current_user_recently_played(after=hour_ago)

    song_names = []
    artist_names = []
    played_at_UTC = []
    played_at_CET = []
    played_at_CEST = []
    date_UTC = []
    date_CET = []
    date_CEST = []
    # Yes, I hate timezones, how did you know?

    for song in recent["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])

        UTC_time = datetime.strptime(song["played_at"][:10] +
                                     ' ' + song["played_at"][11:19], '%Y-%m-%d %H:%M:%S')
        CET_time = UTC_time + timedelta(hours=1)
        CEST_time = UTC_time + timedelta(hours=2)

        played_at_UTC.append(UTC_time)
        played_at_CET.append(CET_time)
        played_at_CEST.append(CEST_time)

        date_UTC.append(UTC_time.strftime('%Y-%m-%d'))
        date_CET.append(CET_time.strftime('%Y-%m-%d'))
        date_CEST.append(CEST_time.strftime('%Y-%m-%d'))

    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at_UTC": played_at_UTC,
        "played_at_CET": played_at_CET,
        "played_at_CEST": played_at_CEST,
        "date_UTC": date_UTC,
        "date_CET": date_CET,
        "date_CEST": date_CEST,
    }

    recent_df = pd.DataFrame(song_dict)
    convert = {'date_UTC': "datetime64[ns]",
               'date_CET': "datetime64[ns]",
               'date_CEST': "datetime64[ns]"
               }
    recent_df = recent_df.astype(convert)

    if is_valid(recent_df):
        print("Data frame is valid, proceeding")

    client = bigquery.Client()
    table_id = "<PROJECT>.<DATASET>.<TABLE>"
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("song_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("artist_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("played_at_UTC", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("played_at_CET", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("played_at_CEST", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("date_UTC", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("date_CET", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("date_CEST", "DATE", mode="REQUIRED")
        ],
        write_disposition="WRITE_APPEND"
    )

    job = client.load_table_from_dataframe(
        recent_df, table_id, job_config=job_config)
    job.result()

    print("Data inserted")