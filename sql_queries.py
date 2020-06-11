import configparser

# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = """CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    first_name VARCHAR(30),
    gender VARCHAR,
    item_in_session INT,
    last_name VARCHAR(30),
    length NUMERIC,
    level VARCHAR(10),
    location VARCHAR,
    method VARCHAR(3),
    page VARCHAR(30),
    registration TIMESTAMP,
    session_id INT,
    song VARCHAR,
    status INT,
    ts TIMESTAMP,
    user_agent VARCHAR,
    user_id VARCHAR
);"""

staging_songs_table_create = """CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs        INTEGER,
    artist_id        VARCHAR, 
    artist_latitude  NUMERIC,
    artist_longitude NUMERIC,
    artist_location  VARCHAR,
    artist_name      VARCHAR,
    song_id          VARCHAR,
    title            VARCHAR,
    duration         NUMERIC,
    year             SMALLINT
);"""

time_table_create = """CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP NOT NULL PRIMARY KEY,
    hour SMALLINT NOT NULL,
    day SMALLINT NOT NULL,
    week SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    year SMALLINT NOT NULL,
    weekday SMALLINT NOT NULL
);"""

user_table_create = """CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR NOT NULL PRIMARY KEY,
    first_name VARCHAR(30),
    last_name VARCHAR(30),
    gender VARCHAR(2),
    level VARCHAR(10)
);"""

song_table_create = """CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR NOT NULL PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year SMALLINT,
    duration NUMERIC
);"""

artist_table_create = """CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude NUMERIC,
    longitude NUMERIC
);"""

songplay_table_create = """CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0, 1) PRIMARY KEY, 
    start_time TIMESTAMP REFERENCES time, 
    user_id VARCHAR REFERENCES users, 
    level VARCHAR(10),
    song_id VARCHAR REFERENCES songs,
    artist_id VARCHAR REFERENCES artists,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR
    );"""


# STAGING TABLES

staging_events_copy = """COPY staging_events FROM '{}' 
JSON '{}' 
CREDENTIALS 'aws_iam_role={}' COMPUPDATE OFF TIMEFORMAT AS 'epochmillisecs' region 'us-west-2';
""".format(
    config["S3"]["LOG_DATA"], config["S3"]["LOG_JSONPATH"], config["IAM_ROLE"]["ARN"]
)

staging_songs_copy = """COPY staging_songs FROM '{}'
JSON 'auto' CREDENTIALS 'aws_iam_role={}' COMPUPDATE OFF 
region 'us-west-2';
""".format(
    config["S3"]["SONG_DATA"], config["IAM_ROLE"]["ARN"]
)

# FINAL TABLES

songplay_table_insert = """INSERT INTO songplays(
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
) 
SELECT ts AS start_time, user_id, level, song_id, artist_id, session_id, location, user_agent FROM staging_events se
JOIN 
staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration 
WHERE se.page='NextSong';"""

user_table_insert = """INSERT INTO users(user_id, first_name, last_name, gender, level) 
SELECT se.user_id, se.first_name, se.last_name, se.gender, se.level
FROM staging_events se;
"""

# NOTE: CHOOSING NULLs to avoid losing more than 4000 rows and in absence of more detailed information
song_table_insert = """INSERT INTO songs (song_id, title, artist_id, year, duration) 
SELECT song_id, title, artist_id, 
CASE WHEN ss.year != 0 
THEN ss.year ELSE NULL 
END AS year, 
duration FROM staging_songs ss;
"""

artist_table_insert = """INSERT INTO artists(artist_id, name, location, latitude, longitude) 
SELECT artist_id, artist, artist_location, artist_latitude, artist_longitude FROM staging_events se 
JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name;"""

time_table_insert = """INSERT INTO time(
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT 
    ts AS start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(dow FROM start_time) AS weekday
FROM (
  SELECT 
  DISTINCT se.ts
  from staging_events se
);
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
