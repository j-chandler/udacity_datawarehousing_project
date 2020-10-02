import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
drop_table = "DROP TABLE IF EXISTS {}"

staging_events_table_drop = drop_table.format("staging_events")
staging_songs_table_drop = drop_table.format("staging_songs")
songplay_table_drop = drop_table.format("songplays")
user_table_drop = drop_table.format("users")
song_table_drop = drop_table.format("songs")
artist_table_drop = drop_table.format("artists")
time_table_drop = drop_table.format("time")

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar,
        auth varchar NOT NULL,
        firstName varchar,
        gender char(1),
        itemInSession int NOT NULL,
        lastName varchar,
        length numeric,
        level varchar NOT NULL,
        location varchar,
        method varchar NOT NULL,
        page varchar NOT NULL,
        registration numeric,
        sessionId int NOT NULL,
        song varchar,
        status int NOT NULL,
        ts numeric NOT NULL,
        userAgent varchar,
        userId int
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs int NOT NULL,
        artist_id varchar NOT NULL,
        artist_latitude numeric,
        artist_longitude numeric,
        artist_location varchar,
        artist_name varchar NOT NULL,
        song_id char varchar NOT NULL,
        title varchar NOT NULL,
        duration numeric NOT NULL,
        year int NOT NULL
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp,
        user_id int NOT NULL,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id int NOT NULL,
        location varchar,
        user_agent varchar,
        UNIQUE(start_time, user_id),
        CONSTRAINT fk_song_id
            FOREIGN KEY(song_id)
            REFERENCES songs(song_id)
            ON DELETE CASCADE,
        CONSTRAINT fk_artist_id
            FOREIGN KEY(artist_id)
            REFERENCES artists(artist_id) 
            ON DELETE CASCADE,
        CONSTRAINT fk_user_id
            FOREIGN KEY(user_id)
            REFERENCES users(user_id)
            ON DELETE CASCADE,
        CONSTRAINT fk_start_time
            FOREIGN KEY(start_time)
            REFERENCES time(start_time)
            ON DELETE CASCADE
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id int PRIMARY KEY,
        first_name varchar,
        last_name varchar,
        gender char(1),
        level varchar
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id varchar PRIMARY KEY,
        title varchar,
        artist_id varchar NOT NULL,
        year int,
        duration numeric,
        CONSTRAINT fk_artist_id
            FOREIGN KEY(artist_id)
            REFERENCES artists(artist_id)
            ON DELETE CASCADE
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id varchar PRIMARY KEY,
        name varchar,
        location varchar,
        latitude numeric,
        longitude numeric
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp PRIMARY KEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(
    start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    ) 
    SELECT
        TO_TIMESTAMP(se.ts) AS start_time,
        se.userId AS user_id,
        se.level AS level,
        ss.song_id AS song_id,
        ss.artist_id AS artist_id,
        se.sessionId AS session_id,
        se.location AS location,
        se.userAgent AS user_agent
    FROM staging_events se
    JOIN staging_songs ss
    ON ss.artist_name = se.artist AND ss.title = se.song AND se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users(
        user_id, first_name, last_name, gender, level
    )
    SELECT
        max_ts_user.user_id AS user_id,
        firstName,
        lastName,
        gender,
        level
    FROM (
        SELECT
            user_id,
            MAX(ts)
        FROM staging_events
        WHERE page = 'NextSong'
        GROUP BY user_id
    ) max_ts_user
    JOIN staging_events se 
    ON se.user_id = max_ts_user.user_id AND se.ts = max_ts_user.ts
""")

song_table_insert = ("""
    INSERT INTO songs
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists(
        artist_id, name, location, latitude, longitude
    )
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
""")


time_table_insert = ("""
    INSERT INTO time(
        start_time, hour, day, week, month, year, weekday
    )
    SELECT
        t_stamp.time,
        EXTRACT(hour from t_stamp.time),
        EXTRACT(day from t_stamp.time),
        EXTRACT(week from t_stamp.time),
        EXTRACT(month from t_stamp.time),
        EXTRACT(year from t_stamp.time),
        EXTRACT(weekday from t_stamp.time)
    FROM (
        SELECT
            TO_TIMESTAMP(ts) time
        FROM staging_events
        WHERE page = 'NextSong'
    ) t_stamp
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, time_table_create, artist_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
