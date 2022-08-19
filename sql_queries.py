import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_event"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_event(
    artist VARCHAR(MAX),
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length DECIMAL,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration BIGINT,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts VARCHAR,
    userAgent TEXT,
    userId INT
    )
    """)

staging_songs_table_create = ("""
    CREATE TABLE staging_song(
     song_id VARCHAR,   
     num_songs INT,
     title VARCHAR,
     artist_name VARCHAR(MAX),
     artist_latitude FLOAT,
     year INT,
     duration FLOAT,
     artist_id VARCHAR,
     artist_longitude FLOAT,
     artist_location VARCHAR(MAX)   
     
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplay(
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY ,
    start_time BIGINT sortkey,
    user_id INT NOT NULL distkey,
    level TEXT,
    song_id TEXT NOT NULL,
    artist_id TEXT NOT NULL,
    session_id INT,
    location TEXT,
    user_agent TEXT
    )
 """)

user_table_create = ("""
    CREATE TABLE users(
    user_id INT NOT NULL,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    level TEXT
    ) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE song(
    song_id TEXT NOT NULL,
    title TEXT,
    artist_id TEXT,
    year INT sortkey,
    duration DECIMAL(9)
    ) diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE artist(
    artist_id TEXT NOT NULL,
    name TEXT,
    location VARCHAR(1024),
    lattitude FLOAT,
    longtitude FLOAT
    );
""")

time_table_create = ("""
    CREATE TABLE time(
    start_time TIMESTAMP NOT NULL sortkey ,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday BOOLEAN
    ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_event
    FROM {}
    IAM_ROLE {}
    format as JSON {};
""").format(config['S3'].get('LOG_DATA'),config['IAM_ROLE'].get('ARN'),config['S3'].get('LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_song
    FROM {}
    IAM_ROLE {}
    COMPUPDATE OFF
    TIMEFORMAT as 'epochmillisecs'
    JSON 'auto'
""").format(config['S3'].get('SONG_DATA'),config['IAM_ROLE'].get('ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)     
    SELECT DISTINCT
           TO_NUMBER(se.ts,'9999999999999') as start_time,
           se.userId,
           se.level,
           ss.song_id,
           ss.artist_id,
           se.sessionId,
           se.location,
           se.userAgent
           
    FROM staging_event se
    Join staging_song ss ON (se.artist=ss.artist_name) AND (se.song=ss.title)
    
""")

user_table_insert = ("""
    INSERT INTO users (user_id,first_name,last_name,gender,level)
    SELECT DISTINCT
           se.userid AS user_id,
           se.firstName,
           se.lastName,
           se.gender,
           se.level
    FROM staging_event se
    WHERE page='NextSong'
""")

song_table_insert = ("""
    INSERT INTO song (song_id,title,artist_id,year,duration)
    SELECT DISTINCT
           ss.song_id AS song_id,
           ss.title,
           ss.artist_id,
           ss.year,
           ss.duration
    FROM staging_song ss
""")

artist_table_insert = ("""
    INSERT INTO artist (artist_id,name,location,lattitude,longtitude)
    SELECT DISTINCT
           ss.artist_id AS artist_id,
           ss.artist_name AS name,
           ss.artist_location,
           ss.artist_latitude,
           ss.artist_longitude
           
    FROM staging_song ss
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT ts, EXTRACT(HOUR FROM ts), EXTRACT(DAY FROM ts), EXTRACT(WEEK FROM ts), EXTRACT(MONTH FROM ts), EXTRACT(YEAR FROM ts), EXTRACT(WEEKDAY FROM ts)
    FROM( 
       SELECT (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ') as ts
       FROM staging_event)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
