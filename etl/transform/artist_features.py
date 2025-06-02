
import pandas as pd
from api_calls import get_track_id, get_track_metadata, get_artist_metadata

def prepare_artist_track_row(sp, row, is_spotify=False, existing_artist_ids=None, existing_track_ids=None):

    if is_spotify:
        track_name = row['title']
        artist_name = row['artist']
    else:
        track_name = row['Song']
        artist_name = row['Artist']

    existing_artist_ids = existing_artist_ids or set()
    existing_track_ids = existing_track_ids or set()

    artist_data = {
        'artist_spotify_id': None,
        'artist_name': artist_name,
        'artist_genre': None,
        'artist_country': None,
        'artist_type': None
    }

    track_data = {
        'track_name': track_name,
        'artist_name': artist_name,
        'featuring_artists': None,
        'artist_count': None,
        'duration': None,
        'explicit': None,
        'spotify_track_id': None,
        'genre': None
    }

    track_id = get_track_id(sp, track_name, artist_name)

    if not track_id:
        return pd.DataFrame(), pd.DataFrame()

    if track_id in existing_track_ids:
        return pd.DataFrame(), pd.DataFrame()

    track_metadata = get_track_metadata(sp, track_id)
    if track_metadata:
        track_data.update({
            'spotify_track_id': track_metadata['track_id'],
            'duration': track_metadata['duration'],
            'explicit': track_metadata['explicit'],
            'featuring_artists': track_metadata['featuring_artists'],
            'artist_count': track_metadata['artist_count'],
            'genre': track_metadata['genres']
        })
        artist_data['artist_spotify_id'] = track_metadata['artist_id']
        artist_data['artist_genre'] = track_metadata['genres']

    if artist_data['artist_spotify_id'] in existing_artist_ids:
        return pd.DataFrame(), pd.DataFrame([track_data])

    if artist_data['artist_spotify_id'] and artist_data['artist_spotify_id'] not in existing_artist_ids:
        musicbrainz_data = get_artist_metadata(artist_name)
        if musicbrainz_data:
            artist_data['artist_country'] = musicbrainz_data['country']
            artist_data['artist_type'] = musicbrainz_data['type']

    return pd.DataFrame([artist_data]), pd.DataFrame([track_data])