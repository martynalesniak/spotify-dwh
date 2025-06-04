import pandas as pd
import holidays
from region import *
from date_features import *

def transform(file_path, country_column=None, date_column=None):
    """
    Wczytuje plik, przetwarza dane poprzez ekstrakcję cech regionu i daty.

    Args:
        file_path (str): Ścieżka do pliku (np. CSV).
        country_column (str): Nazwa kolumny zawierającej kraje (jeśli istnieje).
        date_column (str): Nazwa kolumny zawierającej daty (jeśli istnieje).

    Returns:
        pd.DataFrame: Przetworzony DataFrame.
    """
    df = pd.read_csv(file_path)

    if country_column:
        df = extract_region_features(df, country_column)
    if date_column:
        df = extract_date_features(df, date_column)

    return df
