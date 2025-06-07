CREATE TABLE dim_date (
    date_id INT PRIMARY KEY,  -- Format YYYYMMDD, np. 20241225
    year SMALLINT,
    month SMALLINT,
    day SMALLINT,
    day_of_week VARCHAR(10),
    quarter SMALLINT,
    season VARCHAR(10),
    week_of_year SMALLINT,
    is_holiday BOOLEAN,
    is_weekend BOOLEAN,
    date DATE,
    holiday_name VARCHAR(100)
);

CREATE TABLE dim_track (
    track_id BIGINT IDENTITY(1,1) PRIMARY KEY,  -- Auto-increment surrogate key
    spotify_track_id VARCHAR(22) UNIQUE, 
    track_name VARCHAR(255),
    artist_name VARCHAR(100),
    artist_count SMALLINT,
    duration FLOAT,
    explicit BOOLEAN,
    release_date DATE
);

CREATE TABLE dim_artist (
    artist_id BIGINT IDENTITY(1,1) PRIMARY KEY,  -- Auto-increment surrogate key
    artist_spotify_id VARCHAR(22) UNIQUE,  -- Klucz biznesowy
    artist_name VARCHAR(255),
    artist_genre VARCHAR(60),
    artist_country VARCHAR(100),
    artist_type VARCHAR(50)
);

CREATE TABLE dim_region (
    region_id BIGINT IDENTITY(1,1) PRIMARY KEY,  -- Auto-increment surrogate key
    region_code VARCHAR(10) UNIQUE,  -- Klucz biznesowy
    region_name VARCHAR(150),
    region_population INT,
    region_language VARCHAR(50),
    region_continent VARCHAR(50),
    is_current BOOLEAN DEFAULT TRUE,
    valid_from DATE,
    valid_to DATE
);

CREATE TABLE fact_chart (
    chart_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    track_key BIGINT,
    artist_key BIGINT,
    date_key INT,  -- Zmienione z BIGINT na INT
    region_key BIGINT,
    source_type VARCHAR(50),
    chart_type VARCHAR(50),
    rank SMALLINT,
    weeks_on_chart SMALLINT,
    peak_position SMALLINT,
    position_change SMALLINT
    FOREIGN KEY (track_key) REFERENCES dim_track(track_id),
    FOREIGN KEY (artist_key) REFERENCES dim_artist(artist_id),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_id),
    FOREIGN KEY (region_key) REFERENCES dim_region(region_id)
);