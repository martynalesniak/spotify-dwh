import pandas as pd
import holidays
import world_bank_data as wb
import pycountry
from datetime import datetime
import ast
from date_features import *
from region import get_country_code, continent_mapping, country_to_language


class ChartPreprocessor:
    def __init__(self, region="Global", chart_type="Billboard"):
        self.region = region
        self.chart_type = chart_type
       

    def transform(self, df):
        df = df.copy()

        # Dodaj 'region' jeśli nie istnieje
        if 'region' not in df.columns:
            df['region'] = self.region

        # Dodaj 'chart_type' jeśli nie istnieje
        if 'chart_type' not in df.columns:
            df['chart_type'] = self.chart_type

        # Zamień 'Last Week' na 'previous_rank'
        if 'Last Week' in df.columns:
            df['previous_rank'] = df['Last Week']
            df.drop(columns=['Last Week'], inplace=True)

        # Usuń 'Image URL' jeśli istnieje
        if 'Image URL' in df.columns:
            df.drop(columns=['Image URL'], inplace=True)

        if 'spotify_track_id' not in df.columns:
            df['spotify_track_id'] = df['track_id']

        df['weeks_on_chart'] = df['weeks_on_chart'].replace('-', 0)

    

        return df



def safe_literal_eval(val):
    if pd.isna(val):
        return []
    try:
        return ast.literal_eval(val)
    except (ValueError, SyntaxError):
        return []
    
class DateDimension:
    def __init__(self, df: pd.DataFrame, date_column: str):
        self.df = df.copy()
        self.date_column = date_column

    def transform(self):
        df = self.df.copy()

        # Jeśli date to tylko rok (np. '2025'), dodaj '-01-01'
        df['date'] = df['date'].apply(lambda x: x + '-01-01' if len(x) == 4 else x)

        # Konwertuj do daty
        df['date'] = pd.to_datetime(df['date'])

        country_holidays = holidays.country_holidays('US')

        df['date'] = df[self.date_column]
        df['year'] = df[self.date_column].dt.year
        df['month'] = df[self.date_column].dt.month
        df['day'] = df[self.date_column].dt.day
        df['day_of_week'] = df[self.date_column].dt.day_name()
        df['quarter'] = df[self.date_column].dt.quarter
        df['week_of_year'] = df[self.date_column].dt.isocalendar().week
        df['is_weekend'] = df[self.date_column].dt.weekday.isin([5, 6])
        df['season'] = df[self.date_column].dt.month.map(self.get_season)
        df['is_holiday'] = df[self.date_column].apply(lambda x: x in country_holidays)
        df['holiday_name'] = df[self.date_column].apply(lambda x: country_holidays.get(x, None))
        df['date_id'] = df['date'].dt.strftime('%Y%m%d').astype(int)


        return df[['date_id', 'year', 'month', 'day', 'day_of_week', 'quarter', 'season',
                   'week_of_year', 'is_holiday', 'is_weekend', 'date', 'holiday_name']].drop_duplicates()
    


    @staticmethod
    def get_season(month):
        return {
            12: 'Winter', 1: 'Winter', 2: 'Winter',
            3: 'Spring', 4: 'Spring', 5: 'Spring',
            6: 'Summer', 7: 'Summer', 8: 'Summer',
            9: 'Autumn', 10: 'Autumn', 11: 'Autumn'
        }.get(month, 'Unknown')


class RegionDimension:
    def __init__(self, df: pd.DataFrame, region_column: str):
        self.df = df.copy()
        self.region_column = region_column

    def transform(self):
        df = self.df.copy()
        df[self.region_column] = df[self.region_column].astype(str)
        df['region_code'] = df[self.region_column].apply(get_country_code)
        df['region_continent'] = df[self.region_column].apply(continent_mapping)
        df['region_language'] = df[self.region_column].apply(country_to_language)

        # Pobieranie populacji z World Bank
        population = wb.get_series('SP.POP.TOTL', mrv=1)
        population = population.reset_index().rename(columns={
            'Country': 'Country',
            'SP.POP.TOTL': 'Population'
        })
        population['region_code'] = population['Country'].apply(get_country_code)

        df = df.merge(population[['region_code', 'Population']], on='region_code', how='left')

        # Obsługa w przypadku regionu 'Global'
        if (df['region_code'] == 'WLD').any():
            world_population = population[population['region_code'] == 'WLD']['Population'].values[0]
            df.loc[df['region_code'] == 'WLD', 'Population'] = world_population

        # Obsługa Taiwanu
        taiwan_population = 23595274
        df.loc[df['region_code'] == 'TWN', 'Population'] = taiwan_population

        df['region_population'] = df['Population'].astype(int).fillna(0)

        df['is_current'] = True
        df['valid_from'] = pd.to_datetime('2000-01-01')
        df['valid_to'] = pd.to_datetime('2099-12-31')

        df['region_id'] = pd.factorize(df[self.region_column])[0] + 1
        return df[['region_id', self.region_column, 'region_code', 'region_population', 'is_current',
                   'valid_from', 'valid_to',
                   'region_language', 'region_continent']].rename(columns={self.region_column: 'region_name'}).drop_duplicates()


class ArtistDimension:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def transform(self):
        df = self.df.copy()

        # Sprawdzenie, czy wszystkie potrzebne kolumny istnieją
        if 'genres' not in df.columns:
            return pd.DataFrame()
        
        df = df[df['genres'].notna()]



        # czyszczenie i normalizacja nazw artystów
        df['artist_name'] = df['artist_name'].astype(str).str.strip()
        df['artist_spotify_id'] = df['artist_id'].astype(str).str.strip()

        # genres
        df['genres'] = df['genres'].apply(safe_literal_eval)

        # Weź tylko pierwszy gatunek lub pusty string, jeśli brak
        df['artist_genre'] = df['genres'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else "Unknown")

        df['artist_country'] = df['country'].fillna("Unknown")
        df['artist_type'] = df['type'].fillna('Unknown')

        df['artist_id'] = pd.factorize(df['artist_name'])[0] + 1

        artist_dim = df[['artist_id', 'artist_spotify_id', 'artist_name', 
                         'artist_genre', 'artist_country', 'artist_type']].drop_duplicates()

        return artist_dim



class TrackDimension:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
    
    def transform(self):
        df = self.df.copy()

        # czy mamy kolmne release_date?
        if 'release_date' not in df.columns:
            return pd.DataFrame()
        
        df['release_date'] = df['release_date'].apply(lambda x: x + '-01-01' if len(x) == 4 else x)

        # Konwertuj do daty
        df['release_date'] = pd.to_datetime(df['release_date'])

        df = df[df['release_date'].notna()]


        # Normalizacja i konwersje
        df['track_name'] = df['track_name'].astype(str).str.strip()
        df['artist_name'] = df['artist_name'].astype(str).str.strip()
        df['artist_count'] = df['artist_count'].fillna(1).astype(int)
        df['duration'] = df['duration'].fillna(0).astype(float)
        df['explicit'] = df['explicit'].fillna(False).astype(bool)
        df['spotify_track_id'] = df['spotify_track_id'].astype(str).str.strip()
        # Konwersja release_date z obsługą samych lat
        df['release_date'] = df['release_date'].astype(str).str.strip()
        mask_year_only = df['release_date'].str.match(r'^\d{4}$')
        df.loc[mask_year_only, 'release_date'] = df.loc[mask_year_only, 'release_date'] + '-01-01'

        df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')

        # Usuń wiersze z NaT (czyli niepoprawnymi datami)
        df = df.dropna(subset=['release_date'])

        


        df['track_id'] = pd.factorize(df['track_name'])[0] + 1

        track_dim = df[['track_id', 'track_name', 'artist_name', 'artist_count',
                        'duration', 'explicit', 'spotify_track_id', 'release_date']].drop_duplicates()
        
        return track_dim





class FactChart:
    def __init__(self, df: pd.DataFrame, dim_track: pd.DataFrame, dim_artist: pd.DataFrame, dim_date: pd.DataFrame, dim_region: pd.DataFrame):
        self.df = df.copy()
        self.dim_track = dim_track
        self.dim_artist = dim_artist
        self.dim_date = dim_date
        self.dim_region = dim_region

    def transform(self):
        df = self.df.copy()

       # merge z wymiarem Track
        df = df.merge(self.dim_track[['spotify_track_id', 'track_name', 'artist_name', 'release_date', 'track_id']],
                    left_on=['track_id'],
                    right_on=['spotify_track_id'],
                    how='left')
        df = df.rename(columns={'track_id_y': 'track_key'})

        # merge z wymiarem Artist
        df = df.merge(self.dim_artist[['artist_id', 'artist_spotify_id']], 
                      left_on=['artist_id'],
                        right_on=['artist_spotify_id'],
                        how='left')
        df = df.rename(columns={'artist_id': 'artist_key'})



        # łączenie z wymiarem Date
        df['chart_date'] = pd.to_datetime(df['date'], errors='coerce')
        df['date'] = pd.to_datetime(df['date'])
        self.dim_date['date'] = pd.to_datetime(self.dim_date['date'])
        df = df.merge(self.dim_date[['date_id', 'date']], on='date', how='left')
        df = df.rename(columns={'date_id': 'date_key'})

        # łączenie z wymiarem Region
        df = df.merge(self.dim_region[['region_id', 'region_name']], left_on='region', right_on='region_name', how='left')
        df = df.rename(columns={'region_id': 'region_key'})

        df['rank'] = df['rank'].fillna(0).astype(int)
        df['weeks_on_chart'] = df['weeks_on_chart'].fillna(0).astype(int)

        # Mapowanie position_change
        # previous rank - rank
        df['previous_rank'] = df['previous_rank'].fillna(0).astype(int)
        df['position_change'] = df['previous_rank'] - df['rank'].astype(int)
        df['peak_position'] = df['peak_position'].fillna(0).astype(int)


        df['source_type'] = df['source_type'].fillna('Unknown')
        df['chart_type'] = df['chart_type'].fillna('Unknown')
        print("Kolumny po merge z dim_track:", df.columns.tolist())



        df['release_date'] = pd.to_datetime(df['release_date_y'])


        df['days_since_release'] = (df['chart_date'] - df['release_date']).dt.days
        df['weeks_since_release'] = df['days_since_release'] // 7
        df['months_since_release'] = (df['chart_date'].dt.year - df['release_date'].dt.year) * 12 + (df['chart_date'].dt.month - df['release_date'].dt.month)


        
        df = df.reset_index(drop=True)
        df['chart_id'] = df.index + 1
        print("Kolumny w df przed zwróceniem:", df.columns.tolist())
        df = df.rename(columns={
            'track_id_y': 'track_key',
            'artist_id_y': 'artist_key'
        })



        return df[['chart_id', 'track_key', 'artist_key', 'date_key', 'region_key', 'source_type', 'chart_type', 'rank',
                   'weeks_on_chart', 'peak_position', 'position_change','chart_date', 'release_date', 'days_since_release', 'weeks_since_release', 'months_since_release']].drop_duplicates()
