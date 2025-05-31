import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(r"C:\Users\marty\OneDrive\Pulpit\studia\sem6\hurtownie\spotify-dwh\.env")
load_dotenv(dotenv_path=env_path)


client_id = os.getenv('SPOTIFY_API_KEY')
client_secret = os.getenv('CLIENT_SECRET')

print(load_dotenv(dotenv_path=env_path))  # powinna zwrócić True, jeśli plik znaleziony i załadowany
print(os.getenv('SPOTIFY_API_KEY'))
print(os.getenv('CLIENT_SECRET'))