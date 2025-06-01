import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import os
from dotenv import load_dotenv 

def get_track_id(sp, track_name, artist_name):
    """Takes track_name and artist and returns track_id"""
    query = f"{track_name} {artist_name}"

    results = sp.search(q=query, type='track')

    if not results['tracks']['items']:
        print("No tracks found.")
        return None

    track_id = results['tracks']['items'][0]['id']
    return track_id


def get_audio_features(sp, track_ids):
    """Takes a list of max 100 track_ids and returns a dataframe with audio_features for each id"""
    audio_features_extracted = sp.audio_features(track_ids)
    audio_features_cleaned = []
    for track in audio_features_extracted:
        if track is None:
            continue
        for field in ['uri', 'track_href', 'analysis_url', 'time_signature']:
            track.pop(field, None)
        audio_features_cleaned.append(track)
    return pd.DataFrame(audio_features_cleaned)

def get_genre(sp, track_ids):
    "Takes a list of track_ids and returns a dataframe with list of genres for each id "
    results = []
    for track_id in track_ids:
        try:
            track_info = sp.track(track_id)
            artist_id = track_info['artists'][0]['id']
            artist_info = sp.artist(artist_id)
            genres = artist_info.get('genres', [])
        except Exception:
            genres = []

        results.append({
            'track_id': track_id,
            'genres': genres
        })

    return pd.DataFrame(results)



