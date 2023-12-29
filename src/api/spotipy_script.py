#!/usr/bin/env python
# coding: utf-8

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime
import json
import pandas as pd
import os
import time

# Spotify API credentials
client_id = 'dfb4f42769e24d8a9870f81c6b660989'
client_secret = '0271f1939dc64b6ab8a8bec047d5a9a9'

# Set up authentication using client credentials flow
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Playlist ID - Top 50 Polska
playlist_id = '37i9dQZEVXbN6itCcaL3Tt'

# Funkcja zapisuj¹ca informacje o utworach do pliku JSON
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

def retrive_track_info(playlist_id, frequency=None, json_file_path='nifi_in/songs.json'):
    try:
        # Pobierz top 50 utworów z playlisty
        top_tracks = sp.playlist_tracks(playlist_id, limit=50)

        # SprawdŸ datê dodania pierwszego utworu z playlisty
        first_track_date_added = None
        if top_tracks['items']:
            first_track_date_added = datetime.strptime(top_tracks['items'][0]['added_at'], '%Y-%m-%dT%H:%M:%SZ')

        # SprawdŸ, czy plik JSON istnieje
        existing_tracks = load_existing_tracks(json_file_path)

        # SprawdŸ, czy data dodania utworu istnieje w pliku JSON
        if existing_tracks is not None and 'date_added' in existing_tracks:
            last_track_date_added = existing_tracks['date_added'][-1]
            if first_track_date_added and first_track_date_added.date() == last_track_date_added.date():
                return print("Brak nowych utworow do dodania")

        # Przygotuj listê informacji o utworach do zapisu
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

        # Zapisz informacje o utworach do pliku JSON
        save_to_json(track_info_list, json_file_path)

        # Wyœwietl informacje o utworach
        for track_info in track_info_list:
            print(f"{track_info['rank']}. {track_info['track_name']} by {track_info['artist']} (Popularity: {track_info['popularity']})")

    except spotipy.SpotifyException as e:
        print(f"Spotify API error: {e}")
    except Exception as e:
        print(f"Error: {e}")

    # Czêstotliwoœæ wykonywania pêtli w sekundach
    # time.sleep(frequency)
    return

# Pobieranie utworów do oddzielnej ramki dla danego dnia
retrive_track_info(playlist_id)