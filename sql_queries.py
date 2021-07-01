import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP SCHEMA

sparkify_schema_drop = "DROP SCHEMA IF EXISTS Sparkify;"

# CREATE SCHEMA
sparkify_schema_create = "CREATE SCHEMA IF NOT EXISTS Sparkify;"
sparkify_schema_set = "SET search_path TO Sparkify;"

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS Sparkify.staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS Sparkify.staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS Sparkify.songplays;"
user_table_drop = "DROP TABLE IF EXISTS Sparkify.users;"
song_table_drop = "DROP TABLE IF EXISTS Sparkify.songs;"
artist_table_drop = "DROP TABLE IF EXISTS Sparkify.artists;"
time_table_drop = "DROP TABLE IF EXISTS Sparkify.time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE dev.Sparkify.staging_events (
        id INT IDENTITY(0,1),
        artist VARCHAR(255), 
        auth VARCHAR(50), 
        firstName VARCHAR(100),
        gender VARCHAR(1), 
        itemInSession INTEGER,
        lastName VARCHAR(100),
        length DOUBLE PRECISION,
        level VARCHAR(20),
        location VARCHAR(255),
        method VARCHAR(10),
        page VARCHAR(50),
        registration VARCHAR(100),
        sessionId INTEGER,
        song VARCHAR(255),
        status INTEGER,
        ts BIGINT,
        userAgent VARCHAR(255),
        userId INTEGER,
        PRIMARY KEY (id))
""")

staging_songs_table_create = ("""
    CREATE TABLE dev.Sparkify.staging_songs (
        num_songs INTEGER,
        artist_id VARCHAR(100),
        artist_latitude REAL,
        artist_longitude REAL,
        artist_location VARCHAR(512),
        artist_name VARCHAR(512),
        song_id VARCHAR(100),
        title VARCHAR(255),
        duration REAL,
        year integer)
""")

user_table_create = ("""
    CREATE TABLE dev.Sparkify.users(
        u_user_id INTEGER NOT NULL, 
        u_first_name VARCHAR(100) NOT NULL,
        u_last_name VARCHAR(100) NOT NULL,
        u_gender VARCHAR(1) NOT NULL,
        u_level VARCHAR(20) NOT NULL,
        PRIMARY KEY (u_user_id))
""")

song_table_create = ("""
    CREATE TABLE dev.Sparkify.songs (
        s_song_id VARCHAR(100) NOT NULL, 
        s_title VARCHAR(255) NOT NULL,
        s_artist_id VARCHAR(100) NOT NULL, 
        s_year INTEGER, 
        s_duration REAL,
        PRIMARY KEY (s_song_id))
""")

artist_table_create = ("""
    CREATE TABLE dev.Sparkify.artists(
        a_artist_id VARCHAR(100) NOT NULL,
        a_name VARCHAR(255) NOT NULL,
        a_location VARCHAR(255),
        a_latitude REAL, 
        a_longitude REAL,
        PRIMARY KEY (a_artist_id))
""")

time_table_create = ("""
    CREATE TABLE dev.Sparkify.time(
        t_start_time TIMESTAMP NOT NULL,
        t_hour INTEGER,
        t_day INTEGER,
        t_week INTEGER,
        t_month INTEGER,
        t_year INTEGER,
        t_weekday INTEGER,
        PRIMARY KEY (t_start_time))
""")

songplay_table_create = ("""
    CREATE TABLE dev.Sparkify.songplays (
        sp_id INTEGER IDENTITY(0,1), 
        sp_start_time TIMESTAMP NOT NULL,
        sp_user_id INTEGER NOT NULL,
        sp_level VARCHAR(20) NOT NULL,
        sp_song_id VARCHAR(100) NOT NULL,
        sp_artist_id VARCHAR(100) NOT NULL,
        sp_session_id INTEGER, 
        sp_location VARCHAR(255),
        sp_user_agent VARCHAR(255),
        PRIMARY KEY (sp_id))
""")

# STAGING TABLES

staging_events_copy = ("""
    copy dev.sparkify.staging_events from 's3://udacity-dend/log_data/'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    STATUPDATE OFF
    json 's3://udacity-dend/log_json_path.json';
""").format(*config['IAM_ROLE'].values())

staging_songs_copy = ("""
    copy dev.sparkify.staging_songs from 's3://udacity-dend/song_data/'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    STATUPDATE OFF
    json 'auto';
""").format(*config['IAM_ROLE'].values())

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO dev.Sparkify.songplays (sp_start_time,sp_user_id,sp_level,sp_song_id,sp_artist_id,sp_session_id,sp_location,sp_user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as sp_start_time,                                        
        e.userId,e.level,s.song_id,a.artist_id,e.sessionid,e.location,e.useragent 
    FROM dev.Sparkify.staging_events as e 
    JOIN dev.Sparkify.staging_songs as s ON e.song=s.title
    JOIN dev.Sparkify.staging_songs as a ON e.artist=a.artist_name
    WHERE e.page = 'NextSong' 
""")

user_table_insert = ("""
    INSERT INTO dev.Sparkify.users(u_user_id,u_first_name,u_last_name,u_gender,u_level)
    SELECT DISTINCT userId,firstName,lastName,gender,level 
    FROM dev.Sparkify.staging_events as e
    WHERE e.page='NextSong' AND len(e.userid) > 0
""")

song_table_insert = ("""
    INSERT INTO dev.Sparkify.songs (s_song_id,s_title,s_artist_id,s_year,s_duration)
    SELECT DISTINCT song_id,title,artist_id,year,duration 
    FROM dev.Sparkify.staging_songs 
""")

artist_table_insert = ("""
    INSERT INTO dev.Sparkify.artists(a_artist_id,a_name,a_location,a_latitude,a_longitude)
    SELECT DISTINCT artist_id,artist_name,artist_location,artist_latitude,artist_longitude   
    FROM dev.Sparkify.staging_songs
""")

time_table_insert = ("""
    INSERT INTO dev.Sparkify.time(t_start_time,t_hour,t_day,t_week,t_month,t_year,t_weekday)
    Select
     start_time,
     EXTRACT(HOUR FROM start_time) As hour,
     EXTRACT(DAY FROM start_time) As day,
     EXTRACT(WEEK FROM start_time) As week,
     EXTRACT(MONTH FROM start_time) As month,
     EXTRACT(YEAR FROM start_time) As year, 
     EXTRACT(DOW FROM start_time) As weekday
    FROM
    (
    SELECT DISTINCT TIMESTAMP '1970-01-01'::date + ts/1000 * interval '1 second' as start_time
    FROM dev.Sparkify.staging_events 
    ) 
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
create_schema_queries = [sparkify_schema_create, sparkify_schema_set]
drop_schema_queries = [sparkify_schema_drop]
