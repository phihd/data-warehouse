import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events;"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs;"
songplay_table_drop = "DROP table IF EXISTS songplays;"
user_table_drop = "DROP table IF EXISTS users;"
song_table_drop = "DROP table IF EXISTS staging_songs;"
artist_table_drop = "DROP table IF EXISTS artists;"
time_table_drop = "DROP table IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist        varchar,
        auth          varchar,
        firstName     varchar,
        gender        varchar(1),
        iteminSession int,
        lastName      varchar,
        length        numeric,
        level         varchar,
        location      varchar,
        method        varchar,
        page          varchar,
        registration  varchar,
        sessionId     int,
        song          varchar,
        status        int,
        ts            bigint,
        userAgent     varchar,
        userId        int
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        artist_id varchar,
        artist_latitude numeric,
        artist_location varchar,
        artist_longitude numeric,
        artist_name varchar,
        duration numeric,
        num_songs int,
        song_id varchar,
        title varchar, 
        year int
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id int IDENTITY(0,1) PRIMARY KEY distkey, 
        start_time  timestamp NOT NULL, 
        user_id     int NOT NULL, 
        level       varchar NOT NULL,
        song_id     varchar NOT NULL, 
        artist_id   varchar NOT NULL, 
        session_id  varchar NOT NULL,
        location    varchar NOT NULL, 
        user_agent  varchar NOT NULL
    )
    sortkey auto;
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id    int PRIMARY KEY distkey, 
        first_name varchar NOT NULL, 
        last_name  varchar NOT NULL, 
        gender     varchar NOT NULL, 
        level      varchar NOT NULL
    )
    sortkey auto;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id   varchar PRIMARY KEY distkey, 
        title     varchar NOT NULL, 
        artist_id varchar NOT NULL, 
        year      smallint NOT NULL, 
        duration  numeric NOT NULL
    )
    sortkey auto; 
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id varchar PRIMARY KEY distkey, 
        name      varchar NOT NULL, 
        location  varchar, 
        latitude  numeric, 
        longitude numeric
    )
    sortkey auto; 
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time timestamp PRIMARY KEY distkey, 
        hour smallint NOT NULL, 
        day smallint NOT NULL, 
        week smallint NOT NULL, 
        month smallint NOT NULL, 
        year smallint NOT NULL, 
        weekday smallint NOT NULL
    )
    sortkey auto;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT as json {}
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs from {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    JSON 'auto'
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, 
                          user_id, 
                          level, 
                          song_id, 
                          artist_id, 
                          location, 
                          user_agent, 
                          session_id)
    SELECT e.start_time, 
           e.userId AS user_id, 
           e.level, 
           s.song_id,
           s.artist_id,
           e.location,
           e.userAgent AS user_agent,
           e.sessionId AS session_id
    FROM (
        SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time, *
        FROM staging_events
        WHERE page='NextSong'
    ) e
    JOIN staging_songs s
    ON e.song = s.title
    AND e.artist = s.artist_name
    AND e.length = s.duration
""")

user_table_insert = ("""
    INSERT INTO users(user_id, 
                      first_name, 
                      last_name, 
                      gender, 
                      level)
    WITH uniq_staging_events AS (
        SELECT userId as user_id, 
               firstName AS first_name, 
               lastName AS last_name, 
               gender, 
               level,
               ROW_NUMBER() OVER(PARTITION BY userid ORDER BY ts DESC) AS rank
        FROM staging_events
        WHERE page='NextSong' 
        AND user_id != NULL
    )
    SELECT user_id, 
           first_name, 
           last_name, 
           gender, 
           level
    FROM uniq_staging_events
    WHERE rank = 1;
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, 
                      title, 
                      artist_id, 
                      year, 
                      duration)
    SELECT DISTINCT song_id,
           title,
           artist_id,
           year,
           duration
    FROM staging_songs
    WHERE song_id IS NOT NULL    
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, 
                        name, 
                        location, 
                        longitude, 
                        latitude)
    SELECT DISTINCT artist_id,
                    artist_name,
                    artist_location,
                    artist_longitude,
                    artist_latitude
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time(start_time, 
                     hour, 
                     day, 
                     week, 
                     month, 
                     year, 
                     weekday)
    SELECT DISTINCT t.start_time,
           EXTRACT (HOUR FROM t.start_time),
           EXTRACT (DAY FROM t.start_time),
           EXTRACT (WEEK FROM t.start_time),
           EXTRACT (MONTH FROM t.start_time),
           EXTRACT (YEAR FROM t.start_time),
           EXTRACT (WEEKDAY FROM t.start_time)
    FROM songplays t
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
