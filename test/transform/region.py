import pandas as pd
import numpy as np
from datetime import datetime
import holidays
import pycountry
import world_bank_data as wb


def get_country_code(country_name):
    """
    Funkcja do uzyskania kodu kraju na podstawie nazwy kraju.
    Args:
        country_name (str): Nazwa kraju.
    Returns:
        str: Kod kraju w formacie ISO 3166-1 alpha-2.
    """
    try:
        country = pycountry.countries.lookup(country_name)
        return country.alpha_3
    except LookupError:
        if country_name == 'Global' or country_name == 'World':
            return 'WLD'
        if country_name == 'Hong Kong SAR, China':
            return 'HKG'
        if country_name == 'Egypt, Arab Rep.':
            return 'EGY'
        if country_name == 'Turkiye' or country_name == 'Turkey':
            return 'TUR'
        if country_name == 'Russian Federation' or country_name == 'Russia':
            return 'RUS'
        if country_name == 'Korea, Rep.' or country_name == 'South Korea':
            return 'KOR'
        return 'Unknown'
    
def continent_mapping(country_name):
    country_to_continent = {
    'Argentina': 'South America',
    'Australia': 'Oceania',
    'Brazil': 'South America',
    'Austria': 'Europe',
    'Belgium': 'Europe',
    'Colombia': 'South America',
    'Bolivia': 'South America',
    'Denmark': 'Europe',
    'Bulgaria': 'Europe',
    'Canada': 'North America',
    'Chile': 'South America',
    'Costa Rica': 'North America',
    'Czech Republic': 'Europe',
    'Finland': 'Europe',
    'Dominican Republic': 'North America',
    'Ecuador': 'South America',
    'El Salvador': 'North America',
    'Estonia': 'Europe',
    'France': 'Europe',
    'Germany': 'Europe',
    'Global': 'Global',
    'Greece': 'Europe',
    'Guatemala': 'North America',
    'Honduras': 'North America',
    'Hong Kong': 'Asia',
    'Hungary': 'Europe',
    'Iceland': 'Europe',
    'Indonesia': 'Asia',
    'Ireland': 'Europe',
    'Italy': 'Europe',
    'Israel': 'Asia',
    'India': 'Asia',
    'Japan': 'Asia',
    'Latvia': 'Europe',
    'Lithuania': 'Europe',
    'Malaysia': 'Asia',
    'Morocco': 'Africa',
    'Luxembourg': 'Europe',
    'Mexico': 'North America',
    'Netherlands': 'Europe',
    'New Zealand': 'Oceania',
    'Nicaragua': 'North America',
    'Norway': 'Europe',
    'Panama': 'North America',
    'Paraguay': 'South America',
    'Peru': 'South America',
    'Philippines': 'Asia',
    'Poland': 'Europe',
    'Portugal': 'Europe',
    'Russian Federation': 'Europe',
    'Russia': 'Europe',
    'Singapore': 'Asia',
    'Spain': 'Europe',
    'Saudi Arabia': 'Asia',
    'South Africa': 'Africa',
    'South Korea': 'Asia',
    'Korea, Rep.': 'Asia',
    'Slovakia': 'Europe',
    'Sweden': 'Europe',
    'Taiwan': 'Asia',
    'Switzerland': 'Europe',
    'Turkey': 'Asia',
    'United Kingdom': 'Europe',
    'United States': 'North America',
    'Uruguay': 'South America',
    'Ukraine': 'Europe',
    'United Arab Emirates': 'Asia',
    'Thailand': 'Asia',
    'Andorra': 'Europe',
    'Romania': 'Europe',
    'Vietnam': 'Asia',
    'Egypt': 'Africa'
    }
    return country_to_continent.get(country_name, 'Unknown')

def country_to_language(country_name):
    """
    Funkcja do uzyskania języka kraju na podstawie nazwy kraju.
    Args:
        country_name (str): Nazwa kraju.
    Returns:
        str: Język kraju.
    """
    country_to_language = {
    'Argentina': 'Spanish',
    'Australia': 'English',
    'Brazil': 'Portuguese',
    'Austria': 'German',
    'Belgium': 'Dutch',
    'Colombia': 'Spanish',
    'Bolivia': 'Spanish',
    'Denmark': 'Danish',
    'Bulgaria': 'Bulgarian',
    'Canada': 'English',
    'Chile': 'Spanish',
    'Costa Rica': 'Spanish',
    'Czech Republic': 'Czech',
    'Finland': 'Finnish',
    'Dominican Republic': 'Spanish',
    'Ecuador': 'Spanish',
    'El Salvador': 'Spanish',
    'Estonia': 'Estonian',
    'France': 'French',
    'Germany': 'German',
    'Global': 'English',
    'Greece': 'Greek',
    'Guatemala': 'Spanish',
    'Honduras': 'Spanish',
    'Hong Kong': 'Cantonese',
    'Hungary': 'Hungarian',
    'Iceland': 'Icelandic',
    'Indonesia': 'Indonesian',
    'Ireland': 'English',
    'India': 'Hindi',
    'Israel': 'Hebrew',
    'Italy': 'Italian',
    'Japan': 'Japanese',
    'Latvia': 'Latvian',
    'Lithuania': 'Lithuanian',
    'Malaysia': 'Malay',
    'Morocco': 'Arabic',
    'Luxembourg': 'Luxembourgish',
    'Mexico': 'Spanish',
    'Netherlands': 'Dutch',
    'New Zealand': 'English',
    'Nicaragua': 'Spanish',
    'Norway': 'Norwegian',
    'Panama': 'Spanish',
    'Paraguay': 'Spanish',
    'Peru': 'Spanish',
    'Philippines': 'Filipino',
    'Poland': 'Polish',
    'Portugal': 'Portuguese',
    'Russian Federation': 'Russian',
    'Russia': 'Russian',
    'Singapore': 'English',
    'Saudi Arabia': 'Arabic',
    'South Africa': 'English',
    'South Korea': 'Korean',
    'Korea, Rep.': 'Korean',
    'Spain': 'Spanish',
    'Slovakia': 'Slovak',
    'Sweden': 'Swedish',
    'Taiwan': 'Mandarin',
    'Switzerland': 'German',
    'Turkey': 'Turkish',
    'United Kingdom': 'English',
    'United States': 'English',
    'United Arab Emirates': 'Arabic',
    'Uruguay': 'Spanish',
    'Ukraine': 'Ukrainian',
    'Thailand': 'Thai',
    'Andorra': 'Catalan',
    'Romania': 'Romanian',
    'Vietnam': 'Vietnamese',
    'Egypt': 'Arabic'
    }

    return country_to_language.get(country_name, 'Unknown')



def extract_region_features(df, country_column):

    """
    Funkcja do ekstrakcji cech regionu z kolumny w DataFrame.
    Args:
        df (pd.DataFrame): DataFrame zawierający kolumnę z nazwami krajów.
        country_column (str): Nazwa kolumny zawierającej nazwy krajów.
    Returns:
        pd.DataFrame: DataFrame z dodatkowymi kolumnami zawierającymi cechy regionu.
    """
    df_copy = df.copy()
    
    df_copy[country_column] = df_copy[country_column].astype(str)
    
    df_copy['country_code'] = df_copy[country_column].apply(get_country_code)
    df_copy['continent'] = df_copy[country_column].apply(continent_mapping)
    df_copy['language'] = df_copy[country_column].apply(country_to_language)
    population = wb.get_series('SP.POP.TOTL', mrv=1)
    population = population.reset_index().rename(columns={
    'Country': 'Country',
    'SP.POP.TOTL': 'Population'
    })
    population['country_code'] = population['Country'].apply(get_country_code)


    # obsługa w przypadku region = 'Global'

    df_copy = df_copy.merge(population[['country_code', 'Population']], on='country_code', how='left')
    # Obsługa w przypadku, gdy istnieje rekord z country_code == 'WLD'
    if (df_copy['country_code'] == 'WLD').any():
        world_population = population[population['country_code'] == 'WLD']['Population'].values[0]
        df_copy.loc[df_copy['country_code'] == 'WLD', 'Population'] = world_population

    # Obsługa Taiwanu
    taiwan_poupulation = 23595274
    df_copy.loc[df_copy['country_code'] == 'TWN', 'Population'] = taiwan_poupulation

    return df_copy

