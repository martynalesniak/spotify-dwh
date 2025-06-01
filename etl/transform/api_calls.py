import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
from dotenv import load_dotenv 
import musicbrainzngs

def get_track_id(sp, track_name, artist_name):
    """Takes track_name and artist and returns track_id"""
    query = f"{track_name} {artist_name}"

    results = sp.search(q=query, type='track')

    if not results['tracks']['items']:
        print("No tracks found.")
        return None

    track_id = results['tracks']['items'][0]['id']
    return track_id


def get_duration(sp, track_ids):
    """Takes a list of track_ids and returns a dataframe with duration for each id"""
    results = []
    try:
        response = sp.tracks(track_ids)
        for track in response.get('tracks', []):
            results.append({
                'track_id': track['id'],
                'duration_ms': track.get('duration_ms')
            })
    except Exception as e:
        for track_id in track_ids:
            results.append({
                'track_id': track_id,
                'duration_ms': None
            })

    return pd.DataFrame(results)
def get_genre(sp, track_ids):
    """Takes a list of track_ids and returns a dataframe with list of genres for each id """
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


musicbrainzngs.set_useragent("MyCoolApp", "0.1", "mojemail@example.com")
def get_artist_area(artist_name):
    """
    Search for artist by name and returns his country.
    """
    try:
        result = musicbrainzngs.search_artists(artist=artist_name, limit=15)
        artists = result.get('artist-list', [])
        if not artists:
            print("Artysta nie znaleziony")
            return None
        
        matching_artist = next((a for a in artists if a.get('name', '').lower() == artist_name.lower()), None)

        if not matching_artist:
            print(f"Brak dokładnego dopasowania dla: {artist_name}")
            return None
        
        mbid = matching_artist['id']
        artist_data = musicbrainzngs.get_artist_by_id(mbid)
        artist = artist_data.get("artist", {})
        # print(f"Nazwa:        {artist.get('name', 'brak')}")
        # print(f"Kraj (area):  {artist.get('area', {}).get('name', 'brak')}")
        # print(f"MBID:         {artist.get('id', 'brak')}")
        
        area = artist.get('area', {})
        return area.get('name', None)

    except Exception as e:
        return None
