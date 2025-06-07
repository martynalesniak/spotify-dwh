from extract.extract import EnrichedCSVExtractor
import pandas as pd
import os
from typing import Dict, Any, List
from transform.transform import *
from sqlalchemy import create_engine, text
import glob

class ETLProcessor:

    def __init__(self, base_dir: str = "/opt/airflow/data", sources_dir: str = "/opt/airflow/sources"):
        self.base_dir = base_dir
        self.sources_dir = sources_dir
        self.extracted_dir = os.path.join(base_dir, "extracted")
        self.transformed_dir = os.path.join(base_dir, "transformed")
        
    def get_available_sources(self) -> List[str]:
        """Zwraca listę dostępnych źródeł na podstawie plików w sources_dir"""
        sources = []
        for file in os.listdir(self.sources_dir):
            if file.endswith('.csv'):
                sources.append(os.path.splitext(file)[0])
        return sources
    
    def init_extracted_files(self):
        """Inicjalizuje pliki extracted dla wszystkich wykrytych źródeł"""
        os.makedirs(self.extracted_dir, exist_ok=True)
    
    def extract(self, source_type: str, offset: int, limit: int = 1000) -> Dict[str, Any]:
        """Ekstrahuje dane i zwraca metadane dla XCom"""
        source_path = os.path.join(self.sources_dir, f"{source_type}.csv")

        if not os.path.exists(source_path):
            raise FileNotFoundError(f"Source file not found: {source_path}")
            
        extractor = EnrichedCSVExtractor(source_type)
        df = extractor.extract_data(source_path, offset=offset, limit=limit, enrich=True)
        
        output_path = os.path.join(self.extracted_dir, f"{source_type}_extracted.csv")

        if not df.empty:
            header = not os.path.exists(output_path) or os.path.getsize(output_path) == 0
            df.to_csv(output_path, mode='a', header=header, index=False)
        # Zwracamy metadane dla XCom (małe dane)
        return {
            "source_type": source_type,
            "output_path": str(output_path),
            "record_count": len(df),
            "next_offset": offset + len(df),
            "status": "success" if len(df) > 0 else "no_new_data"
        }
    
    def transform(self, source_type: str, min_records: int = 200) -> Dict[str, Any]:
        """Transformuje dane i zwraca ścieżki plików dla XCom"""
        path = os.path.join(self.extracted_dir, f"{source_type}_extracted.csv")

        if not os.path.exists(path):
            raise FileNotFoundError(f"Extracted file not found: {path}")
            
        df = pd.read_csv(path)
        if len(df) < min_records:
            return {
                "status": "skipped",
                "reason": f"Too few records ({len(df)}) to transform",
                "source_type": source_type
            }
        
        df = ChartPreprocessor().transform(df)

        # Transformacje wymiarów
        dim_date = DateDimension(df, date_column='date').transform()
        dim_region = RegionDimension(df, region_column='region').transform()
        dim_artist = ArtistDimension(df).transform()
        dim_track = TrackDimension(df).transform()
        fact_chart = FactChart(df, dim_track, dim_artist, dim_date, dim_region).transform()

        # Zapis do plików (zachowane oryginalne ścieżki)
        os.makedirs(self.transformed_dir, exist_ok=True)

        
        paths = {
            "dim_date": os.path.join(self.transformed_dir, f"{source_type}_dim_date.csv"),
            "dim_region": os.path.join(self.transformed_dir, f"{source_type}_dim_region.csv"),
            "dim_artist": os.path.join(self.transformed_dir, f"{source_type}_dim_artist.csv"),
            "dim_track": os.path.join(self.transformed_dir, f"{source_type}_dim_track.csv"),
            "fact_chart": os.path.join(self.transformed_dir, f"{source_type}_fact_chart.csv"),
        }

        dim_date.to_csv(paths["dim_date"], index=False)
        dim_region.to_csv(paths["dim_region"], index=False)
        dim_artist.to_csv(paths["dim_artist"], index=False)
        dim_track.to_csv(paths["dim_track"], index=False)
        fact_chart.to_csv(paths["fact_chart"], index=False)

        # Zwracamy metadane dla XCom
        return {
            "status": "success",
            "source_type": source_type,
            "output_files": paths,
            "record_counts": {
                "dim_date": len(dim_date),
                "dim_region": len(dim_region),
                "dim_artist": len(dim_artist),
                "dim_track": len(dim_track),
                "fact_chart": len(fact_chart),
            }
        }

    def load(self, source_type: str, transformed_files: Dict[str, str]) -> Dict[str, Any]:
        """Ładuje dane do hurtowni i zwraca status dla XCom"""
        try:
            db_url = (
                f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}"
                f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
            )
            engine = create_engine(db_url)

            result = {
                "status": "success",
                "source_type": source_type,
                "loaded_tables": [],
                "record_counts": {},
                "details": {}
            }

            # Add debug info about database connection
            try:
                with engine.connect() as conn:
                    # Check if we can connect and query system tables
                    tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
                    existing_tables = pd.read_sql(tables_query, conn)
                    print(f"Existing tables in database: {existing_tables['table_name'].tolist()}")
            except Exception as e:
                print(f"Warning: Could not query database schema: {e}")

            # Kolejność ładowania wymiarów - ważna ze względu na zależności FK
            dim_order = ['dim_date', 'dim_region', 'dim_artist', 'dim_track']

            # Mapowanie nazw plików na nazwy tabel w bazie
            # Use lowercase table names to match what's actually in the database
            table_map = {
                'dim_date': 'dim_date',
                'dim_region': 'dim_region', 
                'dim_artist': 'dim_artist',
                'dim_track': 'dim_track'
            }

            for dim_name in dim_order:
                if dim_name not in transformed_files:
                    continue

                file_path = transformed_files[dim_name]
                df = pd.read_csv(file_path)

                if df.empty:
                    result['details'][dim_name] = "skipped (empty dataframe)"
                    continue

                table_name = table_map[dim_name]

                # Nazwa kolumny z ID w tabelach wymiarów
                id_col = f"{dim_name.replace('dim_', '')}_id"

                # Pobieramy istniejące klucze, by nie duplikować wpisów
                existing_keys_query = f"SELECT {id_col} FROM {table_name}"
                try:
                    existing_keys = pd.read_sql(existing_keys_query, engine)[id_col].values
                    print(f"Found {len(existing_keys)} existing records in {table_name}")
                except Exception as e:
                    print(f"Warning: Could not read existing keys for {table_name}: {e}")
                    existing_keys = []

                # Filtrujemy tylko nowe rekordy
                new_records = df[~df[id_col].isin(existing_keys)]

                if not new_records.empty:
                    # Insert data first
                    try:
                        new_records.to_sql(table_name, engine, if_exists='append', index=False)
                        print(f"Successfully inserted {len(new_records)} records into {table_name}")
                        
                        # Force a small delay to ensure commit is processed
                        import time
                        time.sleep(0.1)
                        
                        # Count records AFTER insertion
                        count_query = f"SELECT COUNT(*) FROM {table_name}"
                        count = pd.read_sql(count_query, engine).iloc[0, 0]
                        print(f"Table {table_name} now has {count} records")
                        
                        # Also try to verify the specific records we just inserted
                        verify_query = f"SELECT COUNT(*) FROM {table_name} WHERE {id_col} IN ({','.join(map(str, new_records[id_col].values))})"
                        verify_count = pd.read_sql(verify_query, engine).iloc[0, 0]
                        print(f"Verification: {verify_count} of our inserted records found in {table_name}")
                        
                    except Exception as insert_error:
                        print(f"Error during insertion into {table_name}: {insert_error}")
                        result['details'][dim_name] = f"failed to insert: {insert_error}"
                        continue
                        
                    result['details'][dim_name] = f"added {len(new_records)} new records"
                else:
                    result['details'][dim_name] = "no new records to add"

                result['record_counts'][dim_name] = len(new_records)
                result['loaded_tables'].append(table_name)

            # Ładowanie tabeli faktów
            if 'fact_chart' in transformed_files:
                fact_df = pd.read_csv(transformed_files['fact_chart'])

                if fact_df.empty:
                    result['details']['fact_chart'] = "skipped (empty dataframe)"
                else:
                    # Sprawdzenie poprawności kluczy obcych w fact_df względem wymiarów
                    existing_dates = pd.read_sql("SELECT date_id FROM dim_date", engine)['date_id'].values
                    fact_df = fact_df[fact_df['date_key'].isin(existing_dates)]

                    existing_regions = pd.read_sql("SELECT region_id FROM dim_region", engine)['region_id'].values
                    fact_df = fact_df[fact_df['region_key'].isin(existing_regions)]

                    existing_artists = pd.read_sql("SELECT artist_id FROM dim_artist", engine)['artist_id'].values
                    fact_df = fact_df[fact_df['artist_key'].isin(existing_artists)]

                    existing_tracks = pd.read_sql("SELECT track_id FROM dim_track", engine)['track_id'].values
                    fact_df = fact_df[fact_df['track_key'].isin(existing_tracks)]

                    if not fact_df.empty:
                        fact_df.to_sql('fact_chart', engine, if_exists='append', index=False)
                        
                        result['details']['fact_chart'] = f"added {len(fact_df)} records"
                        result['record_counts']['fact_chart'] = len(fact_df)
                        result['loaded_tables'].append('fact_chart')
                    else:
                        result['details']['fact_chart'] = "no valid records to add (foreign key constraints)"

            return result

        except Exception as e:
            return {
                "status": "failed",
                "source_type": source_type,
                "error": str(e),
                "loaded_tables": [],
                "record_counts": {}
            }
        
    def cleanup_after_load(self, source_type: str, processed_records_count: int) -> Dict[str, str]:
        """
        Usuwa pliki transformowane i usuwa z pliku extracted dane już załadowane (poza headerem).
        Zwraca info o wykonaniu cleanup.
        """
        try:
            # Ścieżki
            extracted_file_path = os.path.join(self.extracted_dir, f"{source_type}_extracted.csv")

            # 1. Usunięcie plików transformed dla danego source
            transformed_files_pattern = os.path.join(self.transformed_dir, f"{source_type}_*.csv")
            transformed_files = glob.glob(transformed_files_pattern)
            for file_path in transformed_files:
                os.remove(file_path)

            # 2. Odczyt pliku extracted i usunięcie pierwszych processed_records_count wierszy poza headerem
            if not os.path.exists(extracted_file_path):
                return {"status": "failed", "reason": f"Extracted file not found: {extracted_file_path}"}

            df = pd.read_csv(extracted_file_path)

            if len(df) <= processed_records_count:
                df = df.iloc[0:0]  # tylko header
            else:
                df = df.iloc[processed_records_count:]

            df.to_csv(extracted_file_path, index=False)

            return {"status": "success", "message": f"Cleaned extracted file and removed {processed_records_count} records, deleted transformed files."}
        
        except Exception as e:
            return {"status": "failed", "reason": str(e)}