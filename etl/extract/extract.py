import pandas as pd
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import json
import musicbrainzngs
import time
from datetime import datetime
import unicodedata
import re
import traceback
from utils.offset_variable import get_file_offset, update_file_offset
# env_path = r"C:\Users\marty\OneDrive\Pulpit\studia\sem6\hurtownie\spotify-dwh\.env"
#env_path = r'C:\Users\ulasz\OneDrive\Pulpit\studia\sem6\hurtownie danych\spotify-dwh\.env'
#load_dotenv(dotenv_path=env_path)

load_dotenv()

class Cache:
    def __init__(self, cache_path="/opt/airflow/data/cache/id_cache.json"):
        self.cache_path = cache_path
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        if os.path.exists(cache_path):
            with open(cache_path, "r") as f:
                self.data = json.load(f)
        else:
            self.data = {"track_ids": {}, "artist_ids": {}}

    def get_track_data(self, track_name, artist_name):
        key = f"{track_name.strip().lower()}|{artist_name.strip().lower()}"
        return self.data["track_ids"].get(key)

    def set_track_data(self, track_name, artist_name, track_id, artist_id):
        key = f"{track_name.strip().lower()}|{artist_name.strip().lower()}"
        self.data["track_ids"][key] = {
            "track_id": track_id,
            "artist_id": artist_id
        }
    

    def get_artist_status(self, artist_id):
        return self.data["artist_ids"].get(artist_id)

    def set_artist_status(self, artist_id, status):
        key = artist_id
        self.data["artist_ids"][key] = status

    def save(self):
        with open(self.cache_path, "w") as f:
            json.dump(self.data, f, indent=2)

class SpotifyExtractor:
    def __init__(self, client_id=None, client_secret=None):
        self.client_id = client_id or os.getenv('SPOTIFY_API_KEY')
        self.client_secret = client_secret or os.getenv('SPOTIFY_CLIENT_SECRET')
        self.sp = self._setup_spotify_client()
  
    def _setup_spotify_client(self):
        load_dotenv()
        try:
            return spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=self.client_id,
                                                           client_secret=self.client_secret))
        except Exception:
            print("Failed setting up spotify client")
    def get_track_id(self, track_name, artist_name):
        """Takes track_name and artist and returns track_id"""
        query = f"{track_name} {artist_name}"

        results = self.sp.search(q=query, type='track', limit=1)

        if not results['tracks']['items']:
            print(f"No tracks found for {track_name} - {artist_name}")
            return None

        track_id = results['tracks']['items'][0]['id']
        return track_id
    
    def get_track_metadata(self, track_id):
        """Search for track and artist metadata in spotify api """
        try:
            # track metadata
            track_info = self.sp.track(track_id)
            duration = track_info.get('duration_ms', 0) / 1000
            explicit = track_info.get('explicit', False)
            track_name = track_info.get('name', '')
            artists = track_info.get('artists', [])
            artist_count = len(artists)

            artist = artists[0]
            artist_id = artist['id']
            artist_name = artist['name']


            album_info = track_info.get('album', {})
            release_date = album_info.get('release_date', None)

            artist_info = self.sp.artist(artist_id)
            genres = artist_info.get('genres', [])

            return {
                'spotify_track_id': track_id,
                'track_name': track_name,
                'duration': duration,
                'explicit': explicit,
                'artist_id': artist_id,
                'artist_name': artist_name,
                'genres': json.dumps(genres),  # ← teraz jako JSON
                'artist_count': artist_count,
                'release_date': release_date
            }

        except Exception as e:
            print(f"Błąd przy przetwarzaniu track_id {track_id}: {e}")
            return None
        
class MusicBrainzExtractor:
    def __init__(self, delay=0.1):
        self.delay = delay
        musicbrainzngs.set_useragent("MyCoolApp", "0.1", "mojemail@example.com")

    def clean_artist_name(self, name):
        name = name.replace('“', '"').replace('”', '"')
        name = re.sub(r'[‐‑‒–—―]', '-', name)
        name = unicodedata.normalize('NFKD', name)
        return name.strip().lower()
    def get_artist_metadata(self, artist_name):
        try:
            clean_name = self.clean_artist_name(artist_name)
            result = musicbrainzngs.search_artists(artist=artist_name, limit=15)
            artists = result.get('artist-list', [])
            if not artists:
                print("Artists not found.")
                return None
            
            # Użyj clean_name do porównania
            matching_artist = next(
                (a for a in artists if self.clean_artist_name(a.get('name', '')) == clean_name), None
            )

            if not matching_artist:
                print(f"Artist not found for {artist_name}")
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
            print(f"Błąd MusicBrainz dla {artist_name}: {e}")
            return None
        
class CSVExtractor:
    COL_MAPPINGS = {
        "title":"track_name",
        "artist":"artist_name",
        "chart": "chart_type",
        "trend": "position_change",
        "Date":"date",
        "Song":"track_name",
        "Artist":"artist_name",
        "Rank":"rank",
        "Peak Position":"peak_position",
        "Weeks in Charts": "weeks_on_chart"

    }
    def __init__(self, source):
        self.source = source

    def save_to_csv(self, df, output_path) -> None:
        """Save DataFrame to CSV file"""
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.to_csv(output_path, index=False, encoding='utf-8')
            print(f"Data saved to {output_path}, records: {len(df)}")

        except Exception as e:
            print(f"Error saving to {output_path}: {str(e)}")
            raise

    def standardize_columns(self, df):
        df = df.rename(columns={
            k: v for k, v in self.COL_MAPPINGS.items() 
            if k in df.columns
        })
        return df

    def extract_data(self, file_path, offset=0, limit=100):
        try:
            source_type = os.path.basename(file_path).split('.')[0]
            current_offset = offset if offset > 0 else get_file_offset(source_type)
            
            print(f"Reading {limit} rows from offset {current_offset} in {file_path}")
            df = pd.read_csv(file_path, skiprows=range(1, current_offset + 1), nrows=limit)
            df = self.standardize_columns(df)
            df['source_type'] = source_type

            update_file_offset(source_type, current_offset + len(df))
            return df
            
        except Exception as e:
            print(f"Error extracting data: {str(e)}")
            raise

class EnrichedCSVExtractor(CSVExtractor):
    def __init__(self, source):
        super().__init__(source)
        self.spotify_api = SpotifyExtractor()
        self.musicbrainz_api = MusicBrainzExtractor()
        self.cache = Cache()

    def enrich_with_spotify_data(self, df):
        """Enrich DataFrame with Spotify metadata"""

        enriched_data = []
        

        for idx, row in df.iterrows():
            track_name = row.get('track_name')
            artist_name = row.get('artist_name')
            
            if pd.isna(track_name) or pd.isna(artist_name):
                continue
            
            cached_data = self.cache.get_track_data(track_name, artist_name)
            # Get Spotify data
            if cached_data:
                enriched_row = row.to_dict()
                enriched_row['track_id'] = cached_data.get('track_id')
                enriched_row['artist_id'] = cached_data.get('artist_id')
                enriched_data.append(enriched_row)
            else:
                track_id = self.spotify_api.get_track_id(track_name, artist_name)
                if track_id:
                    spotify_metadata = self.spotify_api.get_track_metadata(track_id)
                    if spotify_metadata:
                        # Cacheujemy track_id i artist_id
                        self.cache.set_track_data(track_name, artist_name, track_id, spotify_metadata['artist_id'])
                        if self.cache.get_artist_status(spotify_metadata['artist_id']) is None:
                            self.cache.set_artist_status(spotify_metadata['artist_id'], False)
                        
                        enriched_row = {**row.to_dict(), **spotify_metadata}
                        enriched_data.append(enriched_row)
            
            time.sleep(0.1)
            
            if idx % 10 == 0:
                print(f"Processed {idx}/{len(df)} tracks for Spotify enrichment")
                self.cache.save()
        self.cache.save()
        
        if enriched_data:
            return pd.DataFrame(enriched_data)
        else:
            return df
    
    def enrich_with_musicbrainz_data(self, df):
        """Enrich DataFrame with MusicBrainz metadata"""

        unique_artists_ids = df['artist_id'].dropna().unique()
        artist_metadata = {}
        for idx, artist_id in enumerate(unique_artists_ids):
            if self.cache.get_artist_status(artist_id) is True:
                print(f"Skipping MusicBrainz enrichment for cached artist: {artist_id}")
                continue
            
            artist_name_row = df[df['artist_id'] == artist_id].iloc[0]
            artist_name = artist_name_row['artist_name']

            metadata = self.musicbrainz_api.get_artist_metadata(artist_name)
            if metadata:
                artist_metadata[artist_id] = metadata
            
            self.cache.set_artist_status(artist_id, True)
            
            if idx % 10 == 0:
                print(f"Processed {idx}/{len(unique_artists_ids)} artists for MusicBrainz enrichment")
                self.cache.save()
        
        self.cache.save()
        
        for artist_id, metadata in artist_metadata.items():
            mask = df['artist_id'] == artist_id
            for key, value in metadata.items():
                if key != 'artist_name':
                    df.loc[mask, f'{key}'] = value

        return df
    
    def extract_data(self, file_path, offset=0, limit=100, enrich=True):
        df = super().extract_data(file_path, offset=offset, limit=limit)
        
        if enrich:
            df = self.enrich_with_spotify_data(df)
            df = self.enrich_with_musicbrainz_data(df)

        return df