import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE','ARN')
LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE "staging_events" (
        "artist" text,
        "auth" text,
        "first_name" text,
        "gender" text,
        "item_in_session" integer,
        "last_name" text,
        "length" float4,
        "level" text,
        "location" text,
        "method" text,
        "page" text,
        "registration" float8,
        "session_id" integer,
        "song" text,
        "status" integer,
        "ts" bigint,
        "user_agent" text,
        "user_id" text
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE "staging_songs" (
        "num_songs" integer,
        "artist_id" text,
        "artist_latitude" real,
        "artist_longitude" real,
        "artist_location" text,
        "artist_name" text,
        "song_id" text,
        "title" text,
        "duration" float4,
        "year" smallint
    );
""")

songplay_table_create = ("""
    CREATE TABLE "songplays" (
        "songplay_id" int identity(1,1) primary key,
        "start_time" timestamp not null sortkey,
        "user_id" text not null distkey,
        "level" text,
        "song_id" text,
        "artist_id" text,
        "session_id" integer,
        "location" text,
        "user_agent" text
    ) diststyle key;
""")

user_table_create = ("""
    CREATE TABLE "users" (
        "user_id" text primary key sortkey,
        "first_name" text,
        "last_name" text,
        "gender" text,
        "level" text
    ) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE "songs" (
        "song_id" text primary key sortkey,
        "title" text,
        "artist_id" text distkey,
        "year" smallint,
        "duration" float4
    ) diststyle key;
""")

artist_table_create = ("""
    CREATE TABLE "artists" (
        "artist_id" text primary key sortkey,
        "name" text,
        "location" text,
        "latitude" float4,
        "longitude" float4
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE "time" (
        "start_time" timestamp primary key sortkey,
        "hour" smallint,
        "day" smallint,
        "week" smallint,
        "month" smallint,
        "year" smallint distkey,
        "weekday" smallint
    ) diststyle key;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    IAM_ROLE {}
    JSON {} compupdate off region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    IAM_ROLE {}
    JSON 'auto' compupdate off region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
            timestamp 'epoch' + (ts/1000 * INTERVAL '1 second'),
            user_id,
            level,
            song_id,
            artist_id,
            session_id,
            location,
            user_agent
        FROM staging_events
        LEFT JOIN staging_songs s_songs ON
            staging_events.song = s_songs.title AND
            staging_events.artist = s_songs.artist_name AND
            ABS(staging_events.length - s_songs.duration) < 2
        WHERE staging_events.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users SELECT DISTINCT (user_id)
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM staging_events
""")

song_table_insert = ("""
    INSERT INTO songs SELECT DISTINCT (song_id)
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists SELECT DISTINCT (artist_id)
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time
        WITH temp_time AS (SELECT timestamp 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM staging_events)
        SELECT DISTINCT
        ts,
        extract(hour from ts),
        extract(day from ts),
        extract(week from ts),
        extract(month from ts),
        extract(year from ts),
        extract(weekday from ts)
        FROM temp_time
""")

five_most_played_songs = ("""
    SELECT
        t2.title as "song",
        COUNT(DISTINCT t1.song_id) AS "Song_Play_Count"
    FROM songplays t1
    LEFT JOIN songs t2 ON t1.song_id = t2.song_id
    GROUP BY song
    ORDER BY Song_Play_Count DESC
    LIMIT 5;
""")

five_highest_usage_time_of_day = ("""
    SELECT
        "hour",
        COUNT("hour") AS "usage"
    FROM "time"
    GROUP BY "hour"
    ORDER BY COUNT("hour") DESC
LIMIT 5;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

analytics_queries = [five_most_played_songs, five_highest_usage_time_of_day]
