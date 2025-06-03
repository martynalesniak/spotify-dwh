import pandas as pd
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import json
import musicbrainzngs
import time
from datetime import datetime

load_dotenv()

class SpotifyExtractor:
    def __init__(self, client_id=None, client_secret=None):
        self.client_id = client_id or os.getenv('SPOTIFY_CLIENT_ID')
        self.client_secret = client_secret or os.getenv('CLIENT_SECRET')
        self.sp = self._setup_spotify_client()

    def _setup_spotify_client(self):
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
            print("No tracks found.")
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


            featuring_artists_raw = artists[1:]
            featuring_artists = [
                {
                    'artist_id': fa['id'],
                    'artist_name': fa['name']
                }
                for fa in featuring_artists_raw
            ]

            # dane o artyście (gatunki)
            artist_info = self.sp.artist(artist_id)
            genres = artist_info.get('genres', [])

            return {
                'track_id': track_id,
                'track_name': track_name,
                'duration': duration,
                'explicit': explicit,
                'artist_id': artist_id,
                'artist_name': artist_name,
                'genres': json.dumps(genres),  # ← teraz jako JSON
                'featuring_artists': json.dumps(featuring_artists),
                'artist_count': artist_count
            }

        except Exception as e:
            print(f"Błąd przy przetwarzaniu track_id {track_id}: {e}")
            return None
        
class MusicBrainzExtractor:
    def __init__(self, delay=1.05):
        self.delay = delay
        musicbrainzngs.set_useragent("MyCoolApp", "0.1", "mojemail@example.com")

    def get_artist_metadata(self, artist_name):
        """
        Search for artist by name and returns his country.
        """
        try:
            result = musicbrainzngs.search_artists(artist=artist_name, limit=15)
            artists = result.get('artist-list', [])
            if not artists:
                print("Artists not found.")
                return None
            
            matching_artist = next((a for a in artists if a.get('name', '').lower() == artist_name.lower()), None)

            if not matching_artist:
                print("Artist not found.")
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
        """Standardize column names"""
        df = df.rename(columns={
            k: v for k, v in self.COLUMN_MAPPINGS.items() 
            if k in df.columns
        })
        return df
    def extract_data(self, file_path):
        """Extract and standardize data from CSV"""
        try:
            df = pd.read_csv(file_path)
            df = self.standardize_columns(df)

            filename = os.path.basename(file_path)
            source_type = filename.split('.')[0]
            df['source_type'] = source_type
            
            return df
            
        except Exception as e:
            print("The error occured while extracting csv data")
            raise

class EnrichedCSVExtractor(CSVExtractor):
    def __init__(self, source):
        super().__init__(source)
        self.spotify_api = SpotifyExtractor()
        self.musicbrainz_api = MusicBrainzExtractor()

    def enrich_with_spotify_data(self, df):
        """Enrich DataFrame with Spotify metadata"""

        enriched_data = []
        
        for idx, row in df.iterrows():
            track_name = row.get('track_name')
            artist_name = row.get('artist_name')
            
            if pd.isna(track_name) or pd.isna(artist_name):
                continue
            
            # Get Spotify data
            track_id = self.spotify_api.get_track_id(track_name, artist_name)
            if track_id:
                spotify_metadata = self.spotify_api.get_track_metadata(track_id)
                if spotify_metadata:
                    # Merge original data with Spotify data
                    enriched_row = {**row.to_dict(), **spotify_metadata}
                    enriched_data.append(enriched_row)
            
            # Rate limiting
            time.sleep(0.1)
            
            if idx % 10 == 0:
                print(f"Processed {idx}/{len(df)} tracks for Spotify enrichment")
        
        if enriched_data:
            return pd.DataFrame(enriched_data)
        else:
            return df
    
    def enrich_with_musicbrainz_data(self, df):
        """Enrich DataFrame with MusicBrainz metadata"""

        unique_artists = df['artist_name'].dropna().unique()
        artist_metadata = {}
        
        for artist in unique_artists:
            metadata = self.musicbrainz_api.get_artist_metadata(artist)
            if metadata:
                artist_metadata[artist] = metadata

        for artist, metadata in artist_metadata.items():
            mask = df['artist_name'] == artist
            for key, value in metadata.items():
                if key != 'artist_name':  # Don't overwrite existing artist_name
                    df.loc[mask, f'mb_{key}'] = value
        
        return df
    def extract_data(self, file_path, enrich=True):
        """Extract data with optional API enrichment"""

        df = super().extract_data(file_path)
        
        if enrich and self.enrich_with_apis:
            
            if hasattr(self, 'spotify_api'):
                df = self.enrich_with_spotify_data(df)
            
            if hasattr(self, 'musicbrainz_api'):
                df = self.enrich_with_musicbrainz_data(df)
            
        
        return df
    
def extract_multiple_sources(file_paths, output_dir, enrich=True):
    """Extract data from multiple CSV sources"""
    results = {}
    
    for file_path in file_paths:
        try:
            source_name = os.path.basename(file_path).split('.')[0]
            
            # Choose extractor based on enrichment requirement
            if enrich:
                extractor = EnrichedCSVExtractor(source_name)
            else:
                extractor = CSVExtractor(source_name)
            
            df = extractor.extract_data(file_path, enrich=enrich)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = os.path.join(output_dir, f"raw_{source_name}_{timestamp}.csv")
            extractor.save_to_csv(df, output_path)
            
            results[source_name] = output_path
            
        except Exception as e:
            print(f"Failed to process {file_path}: {str(e)}")
            continue
    
    return results