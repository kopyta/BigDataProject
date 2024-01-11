import requests
import urllib3
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime
import json
import os

# Spotify API credentials
client_id = 'dfb4f42769e24d8a9870f81c6b660989'
client_secret = 'PRIVATE INFO'

session = requests.Session()

retry = urllib3.Retry(
    total=0,
    connect=None,
    read=0,
    allowed_methods=frozenset(['GET', 'POST', 'PUT', 'DELETE']),
    status=0,
    backoff_factor=0.3,
    status_forcelist=(429, 500, 502, 503, 504),
    respect_retry_after_header=False
)

adapter = requests.adapters.HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

sp = spotipy.Spotify(
    auth_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
)

# Playlist ID - Top 50 Polska
playlist_id = '37i9dQZEVXbN6itCcaL3Tt'


def save_to_json(track_info_list, json_file_path):
    with open(json_file_path, 'w', encoding='utf-8') as file:
        json.dump(track_info_list, file, ensure_ascii=False, indent=4)


def load_existing_tracks(json_file_path):
    try:
        with open(json_file_path, 'r', encoding='utf-8') as file:
            existing_tracks = json.load(file)
        return existing_tracks
    except FileNotFoundError:
        return None


def retrieve_track_info(playlist_idd, json_directory='nifi_in/archive'):
    try:
        top_tracks = sp.playlist_tracks(playlist_idd, limit=50)

        first_track_date_added = None
        if top_tracks['items']:
            first_track_date_added = datetime.strptime(top_tracks['items'][0]['added_at'], '%Y-%m-%dT%H:%M:%SZ')

        json_file_path = os.path.join(json_directory, f'songs_{first_track_date_added.date()}.json')

        if os.path.exists(json_file_path):
            print(f"Plik {json_file_path} juz istnieje, brak nowych utworow do dodania")
            return 

        track_info_list = []

        for idx, track in enumerate(top_tracks['items'], start=1):
            track_info = {
                'track_id': track['track']['id'],
                'rank': idx,
                'track_name': track['track']['name'],
                'artist': track['track']['artists'][0]['name'],
                'date_added': datetime.strptime(track['added_at'], '%Y-%m-%dT%H:%M:%SZ').isoformat(),
                'popularity': track['track']['popularity']
            }
            track_info_list.append(track_info)

        stage_file_path = os.path.join('nifi_in/stage', f'songs_{first_track_date_added.date()}.json')
        save_to_json(track_info_list, json_file_path)
        save_to_json(track_info_list, stage_file_path)
        print(f"Plik {json_file_path} zostal pobrany")

    except spotipy.SpotifyException as e:
        print(f"Spotify API error: {e}")
    except Exception as e:
        print(f"Error: {e}")

    return


retrieve_track_info(playlist_id)
