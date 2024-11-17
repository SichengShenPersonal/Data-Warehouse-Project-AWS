import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config.get("IAM","ROLE_ARN")
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS \"user\" "
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
## Staging Tables:
staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location TEXT,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INTEGER
    );
""")

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location TEXT,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent TEXT,
    userId INTEGER
    );
""")

## Fact Table:
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
    songplay_id INTEGER IDENTITY(0,1) NOT NULL PRIMARY KEY,
    user_id INTEGER,
    song_id VARCHAR,
    artist_id VARCHAR,
    start_time TIMESTAMP,
    session_id INTEGER,
    item_in_session INTEGER,
    length FLOAT,
    auth VARCHAR,
    level VARCHAR,
    method VARCHAR,
    location TEXT,
    page VARCHAR,
    user_agent TEXT,
    registration FLOAT,
    status INTEGER
    );
""")

## Dimension Tables:
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS "user" (
    user_id INTEGER NOT NULL PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song (
    song_id VARCHAR NOT NULL PRIMARY KEY,
    artist_id VARCHAR,
    title VARCHAR,
    year INTEGER,
    duration FLOAT
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist (
    artist_id VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR,
    num_songs INTEGER,
    latitude FLOAT,
    longitude FLOAT,
    location TEXT
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP NOT NULL PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday VARCHAR
    );
""")

# STAGING TABLES
staging_songs_copy = ("""
    copy staging_songs 
    from {} 
    credentials 'aws_iam_role={}' 
    format as json 'auto' 
    compupdate off 
    region 'us-west-2';
""").format(SONG_DATA, DWH_ROLE_ARN)

staging_events_copy = ("""
    copy staging_events 
    from {} 
    credentials 'aws_iam_role={}' 
    format as json {} 
    compupdate off 
    region 'us-west-2';
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (
        user_id,
        song_id, 
        artist_id, 
        start_time, 
        session_id,
        item_in_session,
        length,
        auth,
        level,
        method,
        location,
        page,
        user_agent,
        registration,
        status
    ) 
    SELECT
        se.userId,
        ss.song_id, 
        ss.artist_id,
        timestamp 'epoch' + se.ts/1000 * interval '1 second',
        se.sessionId,
        se.itemInSession,
        se.length,
        se.auth,
        se.level,
        se.method,
        se.location,
        se.page,
        se.userAgent,
        se.registration,
        se.status
    FROM staging_events se 
    LEFT JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name)
""")

user_table_insert = ("""
    INSERT INTO "user" (
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level
    ) 
    SELECT 
        DISTINCT userId, 
        firstName, 
        lastName, 
        gender, 
        level
    FROM staging_events
    WHERE page = 'NextSong' AND userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO song (
        song_id,
        artist_id,
        title,
        year, 
        duration
    )
    SELECT 
        DISTINCT song_id, 
        artist_id,
        title,
        year, 
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artist (
        artist_id, 
        name,
        num_songs,
        location, 
        latitude, 
        longitude
    )
    SELECT 
        DISTINCT artist_id, 
        artist_name,
        num_songs,
        artist_location, 
        artist_latitude, 
        artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday
    )
    SELECT 
        DISTINCT start_time, 
        EXTRACT(hour from start_time), 
        EXTRACT(day from start_time), 
        EXTRACT(week from start_time), 
        EXTRACT(month from start_time), 
        EXTRACT(year from start_time), 
        EXTRACT(weekday from start_time)
    FROM songplay
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
