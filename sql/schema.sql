CREATE TABLE dim_Date (
    date_id BIGINT PRIMARY KEY,
    year SMALLINT,
    month TINYINT,
    day TINYINT,
    day_of_week VARCHAR(10),
    quarter TINYINT,
    season VARCHAR(10),
    week_of_year TINYINT,
    is_holiday BOOLEAN,
    is_weekend BOOLEAN,
    date DATE,
    holiday_name VARCHAR(100)
);


CREATE TABLE dim_Track (
    track_id BIGINT PRIMARY KEY,
    track_name VARCHAR(255),
    main_artist_name VARCHAR(100),
    artist_count TINYINT,
    duration INT,
    explicit BOOLEAN,
    genre VARCHAR(100),
    danceability FLOAT,
    energy FLOAT,
    major_of_track VARCHAR(10),
    loudness FLOAT,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
);


CREATE TABLE dim_Artist (
    artist_id BIGINT PRIMARY KEY,
    artist_name VARCHAR(255),
    artist_spotify_id VARCHAR(100),
    artist_genre VARCHAR(100),
    artist_nationality VARCHAR(100),
    artist_type VARCHAR(50)
);

CREATE TABLE dim_Region (
    region_id BIGINT PRIMARY KEY,
    region_name VARCHAR(150),
    region_code VARCHAR(10),
    region_population INT,
    region_language VARCHAR(50),
    region_continent VARCHAR(50),
);

CREATE TABLE Fact_charts (
    chart_id BIGINT PRIMARY KEY,
    track_key BIGINT,
    artist_key BIGINT,
    date_key BIGINT,
    region_key BIGINT,
    source_type VARCHAR(50),
    chart_type VARCHAR(50),
    rank SMALLINT,
    weeks_on_chart SMALLINT,
    peak_position SMALLINT,
    position_change SMALLINT,
    FOREIGN KEY (track_key) REFERENCES dim_Track(track_id),
    FOREIGN KEY (artist_key) REFERENCES dim_Artist(artist_id),
    FOREIGN KEY (date_key) REFERENCES dim_Date(date_id),
    FOREIGN KEY (region_key) REFERENCES dim_Region(region_id)
);
