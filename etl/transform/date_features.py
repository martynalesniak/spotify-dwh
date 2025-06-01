import pandas as pd
import numpy as np
from datetime import datetime
import holidays
import pycountry


def get_season(date):
    """
    Funkcja do określenia sezonu na podstawie daty.
    Args:
        date (datetime): Data, dla której ma zostać określony sezon.
    Returns:
        str: Nazwa sezonu ('Spring', 'Summer', 'Fall', 'Winter').
    """
    Y = 2000  # Rok nie ma znaczenia dla sezonu
    seasons = {
        'Spring': (pd.Timestamp(f'{Y}-03-21'), pd.Timestamp(f'{Y}-06-20')),
        'Summer': (pd.Timestamp(f'{Y}-06-21'), pd.Timestamp(f'{Y}-09-22')),
        'Fall':   (pd.Timestamp(f'{Y}-09-23'), pd.Timestamp(f'{Y}-12-20')),
        'Winter': (pd.Timestamp(f'{Y}-12-21'), pd.Timestamp(f'{Y+1}-03-20'))
    }
    d = pd.Timestamp(f"{Y}-{date.month}-{date.day}")
    for season, (start, end) in seasons.items():
        if start <= d <= end:
            return season
    return 'Winter'


def extract_date_features(df, date_column):
    """
    funkcja do ekstrakcji cech daty z kolumny w DataFrame.
    Args:
        df (pd.DataFrame): DataFrame zawierający kolumnę z datami.
        date_column (str): Nazwa kolumny zawierającej daty w formacie 'YYYY-MM-DD'.
    Returns:
        pd.DataFrame: DataFrame z dodatkowymi kolumnami zawierającymi cechy daty.
    """
    df_copy = df.copy()
    df_copy[date_column] = pd.to_datetime(df_copy[date_column], format='%Y-%m-%d', errors='coerce')

    df_copy = df_copy[df_copy[date_column].notna()]

    country_holidays = holidays.country_holidays('US')
    df_copy['date'] = df_copy[date_column]
    df_copy['year'] = df_copy[date_column].dt.year
    df_copy['month'] = df_copy[date_column].dt.month
    df_copy['day'] = df_copy[date_column].dt.day
    df_copy['day_of_week'] = df_copy[date_column].dt.dayofweek
    df_copy['quarter'] = df_copy[date_column].dt.quarter
    df_copy['season'] = df_copy[date_column].apply(get_season)
    df_copy['is_holiday'] = df_copy[date_column].apply(lambda x: x in country_holidays)
    df_copy['week_of_year'] = df_copy[date_column].dt.isocalendar().week
    df_copy['is_weekend'] = df_copy['day_of_week'].isin([5, 6])
    df_copy['holiday_name'] = df_copy[date_column].apply(lambda x: country_holidays.get(x, None))

    dim_columns = ['date', 'year', 'month', 'day', 'day_of_week', 'quarter', 'season', 'is_holiday', 
                   'week_of_year', 'is_weekend', 'holiday_name']

    return df_copy 