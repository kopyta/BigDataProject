#!/usr/bin/env python
# coding: utf-8

# In[31]:


import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime
import csv
import pandas as pd
import os
import time


# In[32]:


# Spotify API credentials
client_id = 'dfb4f42769e24d8a9870f81c6b660989'
client_secret = '0271f1939dc64b6ab8a8bec047d5a9a9'

# Set up authentication using client credentials flow
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Playlist ID - Top 50 Polska
playlist_id = '37i9dQZEVXbN6itCcaL3Tt'
# '37i9dQZF1DXcBWIGoYBM5M' - Top Global

# Określ nazwę katalogu głównego projektu
project_directory_name = 'BigDataProject'

# Pobierz ścieżkę do katalogu, w którym znajduje się bieżący skrypt (SpotipyScript.ipynb)
current_directory = os.path.dirname(os.path.abspath('__file__')).replace('\\','/')

# Przejście w górę po drzewie katalogów, aż dojdziesz do katalogu projektu
while os.path.basename(current_directory) != project_directory_name:
    current_directory = os.path.dirname(current_directory).replace('\\','/')


# Funkcja zapisująca informacje o utworach do pliku CSV
def save_to_csv(track_info_list, csv_file_path):
    # Sprawdź czy ścieżka kończy się na .csv
    if not csv_file_path.lower().endswith('.csv'):
        # Ustaw typ pliku na csv
        csv_file_path=csv_file_path + ".csv"

    # Pobierz katalog z pełnej ścieżki
    directory = os.path.dirname(csv_file_path)

    # Sprawdź czy katalog istnieje, a jeśli nie, utwórz go (rekurencyjnie)
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Zdefiniuj nagłówek tabeli
    fieldnames = ['track_id','rank', 'track_name', 'artist', 'date_added', 'popularity']

    with open(csv_file_path, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=';')

        # Sprawdź, czy plik CSV jest pusty, a jeśli tak, napisz nagłówki
        if file.tell() == 0:
            writer.writeheader()

        # Zapisz informacje o utworach do pliku CSV
        writer.writerows(track_info_list)


# In[34]:


def load_existing_tracks(csv_file_path):
    # Wczytaj istniejące informacje o utworach z pliku CSV
    try:
        existing_tracks = pd.read_csv(csv_file_path, parse_dates=['date_added'], sep=';')
        return existing_tracks
    except FileNotFoundError:
        return None


# In[35]:


def retrive_track_info(playlist_id,frequency=None, csv_file_path='data/'): 

    # Pętla, która uruchamia się co godzinę
    # while True:
    try:
        # Ustal, czy wartości mają być dodane do istniejącego już pliku
        update_the_file = False
        if not csv_file_path.lower().endswith('/'):
            update_the_file = True
            temp_file_path = csv_file_path
        # Pobierz top 50 utworów z playlisty
        top_tracks = sp.playlist_tracks(playlist_id, limit=50)

        # Sprawdź datę dodania pierwszego utworu z playlisty
        first_track_date_added = None
        if top_tracks['items']:
            first_track_date_added = datetime.strptime(top_tracks['items'][0]['added_at'], '%Y-%m-%dT%H:%M:%SZ')
            if update_the_file==False:
                temp_file_path = csv_file_path + str(first_track_date_added.date()) +".csv"

        # Sprawdź, czy plik CSV istnieje
        existing_tracks = load_existing_tracks(temp_file_path)

        # Sprawdź, czy data dodania utworu istnieje w pliku CSV
        if update_the_file:
            if existing_tracks is not None and not existing_tracks.empty and 'date_added' in existing_tracks:
                last_track_date_added = existing_tracks['date_added'].iloc[-1]
                if first_track_date_added and first_track_date_added.date() == last_track_date_added.date():
                    # Utwórz zmienną informującą o braku nowych danych
                    new_rank_exists = False 
                    return print("Brak nowych utworów do dodania.")
        else: 
            if existing_tracks is not None and not existing_tracks.empty:
                # Utwórz zmienną informującą o braku nowych danych
                new_rank_exists = False 
                return print("Brak nowych utworów do dodania.")
        
        # Zaktualizuj ścieżkę pliku tymczasowego, pobieranego prosto z API (do przetworzenia przez NiFi)
        csv_file_path = csv_file_path + 'songs' +".csv"
        
        # Zaktualizuj zmienną informującą o istnieniu nowych danych
        new_rank_exists = True
        
        if new_rank_exists:
            # Przygotuj listę informacji o utworach do zapisu
            track_info_list = []
               
            for idx, track in enumerate(top_tracks['items'], start=1):
                track_info = {
                    'track_id': track['track']['id'], 
                    'rank': idx,
                    'track_name': track['track']['name'],
                    'artist': track['track']['artists'][0]['name'],
                    'date_added': datetime.strptime(track['added_at'], '%Y-%m-%dT%H:%M:%SZ'),
                    'popularity': track['track']['popularity']
                }
                track_info_list.append(track_info)
            
            # Zapisz informacje o utworach do pliku CSV
            save_to_csv(track_info_list, csv_file_path)
            
            # Wyświetl informacje o utworach
            for track_info in track_info_list:
                print(f"{track_info['rank']}. {track_info['track_name']} by {track_info['artist']} (Popularity: {track_info['popularity']})")

    except spotipy.SpotifyException as e:
        print(f"Spotify API error: {e}")
    except Exception as e:
        print(f"Error: {e}")

    # Częstotliwość wykonywania pętli w sekundach
    # time.sleep(frequency)
    return


# In[36]:


def seperate_tracks_by_date(source_file_path, result_file_path=None):
    # Funkcja rozdziela zbiorczy plik csv z top utworami, na pliki z top utworami dla danego dnia

    # Wczytaj plik CSV, ze sprawdzeniem typu pliku
    if not source_file_path.lower().endswith('.csv'):
        return "Plik nie jest plikiem csv"
    else:
        df = pd.read_csv(source_file_path, parse_dates=['date_added'], sep=';')

    # Zdefiniuj ścieżkę plików wynikowych
    if not result_file_path:
        result_file_path = os.path.dirname(source_file_path) +"/"

    # Grupuj DataFrame według kolumny 'date_added'
    grouped_by_date = df.groupby(df['date_added'].dt.date)

    # Dla każdej grupy utwórz osobny plik CSV
    for date, group in grouped_by_date:
        # Utwórz nazwę pliku na podstawie daty
        file_name = result_file_path + f"{date}.csv"
        
        # Zapisz grupę do pliku CSV
        group.to_csv(file_name, index=False, sep=';')

    print("Pliki CSV zostały utworzone dla każdego unikalnego dnia.")


# In[37]:


# Dopasuj ID utworów na podstawie wiersza z ramki
def get_track_id(row):
    track_name = row['track_name']
    artist = row['artist']
    track_id = row.get('track_id')  # Sprawdź, czy track_id już istnieje

    # Jeśli track_id już istnieje, nie wykonuj wyszukiwania
    if track_id:
        return track_id

    # Wyszukiwanie utworu
    results = sp.search(q=f"track:{track_name} artist:{artist}", type='track', limit=1)

    # Sprawdź, czy znaleziono utwór
    if results['tracks']['items']:
        found_track = results['tracks']['items'][0]
        track_id = found_track['id']
        return track_id
    else:
        return None


# In[38]:


def add_track_id_to_csv(csv_file_path):
    # Wczytaj ramkę danych CSV
    df = load_existing_tracks(csv_file_path)

    # Sprawdź, czy wczytano prawidłową ramkę danych
    if df is not None:
        # Zastosuj funkcję do każdego wiersza w ramce danych
        df['track_id'] = df.apply(get_track_id, axis=1)

        # Przesuń kolumnę 'id' na pierwszą pozycję
        cols = ['track_id'] + [col for col in df if col != 'track_id']
        df = df[cols]

        # Zapisz nową ramkę danych pod starą ścieżką
        df.to_csv(csv_file_path, sep=';', index=False)
        print('Zmodyfikowano ramkę')
    else:
        print("Wczytywanie pliku CSV nie powiodło się.")


# In[39]:


def apply_to_date_files(start_date,end_date,directory_path):
    # Iterate through files in the directory
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)

        try:
            # Check if the file is a CSV file and its name represents a date within the specified range
            if filename.endswith('.csv') and start_date <= filename <= end_date:
                add_track_id_to_csv(file_path)
                print("Done")
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")


# In[41]:


# Pobieranie utworów do oddzielnej ramki dla danego dnia
retrive_track_info(playlist_id)
# %%
