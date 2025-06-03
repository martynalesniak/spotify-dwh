import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
from dotenv import load_dotenv 
import musicbrainzngs
import json

def get_track_id(sp, track_name, artist_name):
    """Takes track_name and artist and returns track_id"""
    query = f"{track_name} {artist_name}"

    results = sp.search(q=query, type='track')

    if not results['tracks']['items']:
        print("No tracks found.")
        return None

    track_id = results['tracks']['items'][0]['id']
    return track_id


def get_track_metadata(sp, track_id):
    """Search for track and artist metadata in spotify api """
    try:
        # track metadata
        track_info = sp.track(track_id)
        duration = track_info.get('duration_ms') / 1000
        explicit = track_info.get('explicit')
        track_name = track_info.get('name')
        artists = track_info['artists']
        artist_count = len(artists)

        artist = artists[0]
        artist_id = artist['id']
        artist_name = artist['name']


        featuring_artists_raw = artists[1:]
        featuring_artists = [
            {
                'artist_id': fa['id'],
                'artist_name': fa['name']
            }
            for fa in featuring_artists_raw
        ]

        # dane o artyście (gatunki)
        artist_info = sp.artist(artist_id)
        genres = artist_info.get('genres', [])

        return {
            'track_id': track_id,
            'track_name': track_name,
            'duration': duration,
            'explicit': explicit,
            'artist_id': artist_id,
            'artist_name': artist_name,
            'genres': json.dumps(genres),  # ← teraz jako JSON
            'featuring_artists': json.dumps(featuring_artists),  # ← też JSON
            'artist_count': artist_count
        }

    except Exception as e:
        print(f"Błąd przy przetwarzaniu track_id {track_id}: {e}")
        return None


musicbrainzngs.set_useragent("MyCoolApp", "0.1", "mojemail@example.com")
def get_artist_metadata(artist_name):
    """
    Search for artist by name and returns his country.
    """
    try:
        result = musicbrainzngs.search_artists(artist=artist_name, limit=30)
        artists = result.get('artist-list', [])
        if not artists:
            print("Artysta nie znaleziony")
            return None
        
        print(f"Znaleziono {len(artists)} artystów dla nazwy '{artist_name}':\n")

        for i, a in enumerate(artists, 1):
            name = a.get('name', 'brak nazwy')
            artist_type = a.get('type', 'brak typu')
            country = a.get('area', {}).get('name', 'brak kraju')
            disambiguation = a.get('disambiguation', '')
            print(f"{i}. {name} ({artist_type}, {country}) {'- ' + disambiguation if disambiguation else ''}")
        matching_artist = next((a for a in artists if a.get('name', '').lower() == artist_name.lower()), None)

        if not matching_artist:
            matching_artist = next((a for a in artists if a.get('sort-name', '').lower() == artist_name.lower()), None)
        if not matching_artist:
            print("Brak dopasowania dla artysty w musibrainz")
            return None
        mbid = matching_artist['id']
        artist_data = musicbrainzngs.get_artist_by_id(mbid)
        artist = artist_data.get("artist", {})
        country = artist.get('area', {}).get('name')
        artist_type = artist.get('type', None)

        return {
            'country': country,
            'type': artist_type
        }

    except Exception as e:
        return None
