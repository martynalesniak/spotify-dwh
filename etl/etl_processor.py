from extract.extract import EnrichedCSVExtractor
import pandas as pd
import os
from typing import Dict, Any, List
from transform.transform import *


class ETLProcessor:

    def __init__(self, base_dir: str = "/data", sources_dir: str = "/opt/airflow/sources"):
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
        for source in self.get_available_sources():
            path = os.path.join(self.extracted_dir, f"{source}_extracted.csv")
            if not os.path.exists(path):
                pd.DataFrame().to_csv(path, index=False)
    
    def extract(self, source_type: str, offset: int, limit: int = 100) -> Dict[str, Any]:
        """Ekstrahuje dane i zwraca metadane dla XCom"""
        source_path = os.path.join(self.sources_dir, f"{source_type}.csv")

        if not os.path.exists(source_path):
            raise FileNotFoundError(f"Source file not found: {source_path}")
            
        extractor = EnrichedCSVExtractor(source_type)
        df = extractor.extract_data(source_path, offset=offset, limit=limit, enrich=True)
        
        output_path = os.path.join(self.extracted_dir, f"{source_type}_extracted.csv")

        if not df.empty:
            header = not os.path.exists(output_path)
            df.to_csv(output_path, mode='a', header=header, index=False)
        # Zwracamy metadane dla XCom (małe dane)
        return {
            "source_type": source_type,
            "output_path": str(output_path),
            "record_count": len(df),
            "next_offset": offset + len(df),
            "status": "success" if len(df) > 0 else "no_new_data"
        }
    
    def transform(self, source_type: str, min_records: int = 1000) -> Dict[str, Any]:
        """Transformuje dane i zwraca ścieżki plików dla XCom"""
        path = os.path.join(self.extracted_dir, f"{source_type}_extracted.csv")

        if not path.exists():
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
        "dim_date": os.path.join(self.transformed_dir, f"{source_type}_dim_date.parquet"),
        "dim_region": os.path.join(self.transformed_dir, f"{source_type}_dim_region.parquet"),
        "dim_artist": os.path.join(self.transformed_dir, f"{source_type}_dim_artist.parquet"),
        "dim_track": os.path.join(self.transformed_dir, f"{source_type}_dim_track.parquet"),
        "fact_chart": os.path.join(self.transformed_dir, f"{source_type}_fact_chart.parquet"),
    }

        dim_date.to_parquet(paths["dim_date"], index=False)
        dim_region.to_parquet(paths["dim_region"], index=False)
        dim_artist.to_parquet(paths["dim_artist"], index=False)
        dim_track.to_parquet(paths["dim_track"], index=False)
        fact_chart.to_parquet(paths["fact_chart"], index=False)

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
        # Tutaj implementacja Twojego ładowania do bazy
        # Przykład:
        try:
            # ... wykonaj ładowanie ...
            return {
                "status": "success",
                "source_type": source_type,
                "loaded_tables": list(transformed_files.keys()),
                "record_counts": {k: pd.read_parquet(v).shape[0] for k, v in transformed_files.items()}
            }
        except Exception as e:
            return {
                "status": "failed",
                "source_type": source_type,
                "error": str(e)
            }