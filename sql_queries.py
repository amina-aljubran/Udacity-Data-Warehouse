import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# CREATE SCHEMAS
fact_schema        = ("CREATE SCHEMA IF NOT EXISTS FACT_SCHEMAS")
dimension_schema   = ("CREATE SCHEMA IF NOT EXISTS DIMENSION_SCHEMAS")
staging_schema     = ("CREATE SCHEMA IF NOT EXISTS STAGING_SCHEMAS")


# DROP SCHEMAS
fact_schema_drop      = ("DROP SCHEMA IF EXISTS FACT_SCHEMAS CASCADE")
dimension_schema_drop = ("DROP SCHEMA IF EXISTS DIMENSION_SCHEMAS CASCADE")
staging_schema_drop   = ("DROP SCHEMA IF EXISTS STAGING_SCHEMAS CASCADE")



# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS STAGING_SCHEMAS.STAGING_EVENTS
                             (event_id      BIGINT IDENTITY(0,1)    PRIMARY KEY,
                              artist        TEXT           NULL,
                              auth          TEXT           NULL,
                              first_name    TEXT           NULL,
                              gender        TEXT           NULL,
                              itemInSession INTEGER        NULL,
                              last_name     TEXT           NULL,
                              length        NUMERIC        NULL,
                              level         TEXT           NULL,
                              location      TEXT           NULL,
                              method        TEXT           NULL,
                              page          TEXT           NULL,
                              registration  BIGINT         NULL,
                              session_id    INTEGER        NOT NULL SORTKEY DISTKEY,
                              song          TEXT           NULL,
                              status        INTEGER        NULL,
                              ts            BIGINT         NOT NULL,
                              user_Agent    TEXT           NULL, 
                              user_id       INTEGER        NULL)
                              """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS STAGING_SCHEMAS.STAGING_SONGS
                             (num_songs          INTEGER   PRIMARY KEY,
                             artist_id           TEXT      NOT NULL SORTKEY DISTKEY,
                             artist_name         TEXT      NULL,
                             artist_latitude     TEXT      NULL,
                             artist_longitude    TEXT      NULL,
                             artist_location     TEXT      NULL,
                             song_id             TEXT      NOT NULL,
                             title               TEXT      NULL,
                             duration            NUMERIC   NULL,
                             year                INTEGER   NULL)
                             """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS FACT_SCHEMAS.SONGPLAYS 
                        (songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
                        artist_id TEXT NOT NULL, 
                        start_time TIMESTAMP NOT NULL, 
                        user_id INTEGER NOT NULL, 
                        level TEXT, 
                        song_id TEXT NOT NULL, 
                        session_id INTEGER NOT NULL, 
                        artist_location TEXT, 
                        user_agent TEXT);
                        """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS DIMENSION_SCHEMAS.USERS 
                    (user_id INTEGER PRIMARY KEY, 
                    first_name TEXT, 
                    last_name TEXT, 
                    gender TEXT, 
                    level TEXT) diststyle all;
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS DIMENSION_SCHEMAS.SONGS 
                    (song_id TEXT PRIMARY KEY, 
                    title TEXT, 
                    artist_id TEXT NOT NULL, 
                    year NUMERIC, 
                    duration NUMERIC);
                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS DIMENSION_SCHEMAS.ARTISTS 
                        (artist_id TEXT PRIMARY KEY, 
                        artist_name TEXT, 
                        artist_location TEXT, 
                        artist_latitude NUMERIC, 
                        artist_longitude NUMERIC) diststyle all;
                        """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS DIMENSION_SCHEMAS.TIME 
                    (start_time TIMESTAMP PRIMARY KEY, 
                    hour INTEGER, 
                    day TEXT, 
                    week TEXT, 
                    month TEXT, 
                    year INTEGER, 
                    weekday TEXT) diststyle all;
                    """)


# STAGING TABLES

staging_events_copy = ("""copy STAGING_SCHEMAS.STAGING_EVENTS 
                      from {}
                      iam_role {}
                      json {}
                      """).format(config['S3']['LOG_DATA'],
                                  config['IAM_ROLE']['ARN'],
                                  config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""copy STAGING_SCHEMAS.STAGING_SONGS 
                      from {}
                      iam_role {}
                      json 'auto'
                      """).format(config['S3']['SONG_DATA'],
                                  config['IAM_ROLE']['ARN'])


# FINAL TABLES

songplay_table_insert = ("""INSERT INTO FACT_SCHEMAS.SONGPLAYS (start_time, user_id, level, song_id, artist_id,
                                                   session_id, artist_location,user_agent)
                            SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time,                                              e.user_id, e.level, s.song_id, s.artist_id, 
                                   e.session_id, e.location, e.user_agent
                            FROM   STAGING_SCHEMAS.STAGING_EVENTS AS e
                            JOIN   STAGING_SCHEMAS.STAGING_SONGS  AS s
                            ON     e.artist = s.artist_name
                            WHERE  e.page = 'NextSong';
                         """)


user_table_insert     = ("""INSERT INTO DIMENSION_SCHEMAS.USERS (user_id, first_name, last_name, gender, level)
                            SELECT DISTINCT e.user_id, e.first_name, e.last_name, e.gender, e.level
                            FROM   STAGING_SCHEMAS.STAGING_EVENTS AS e
                            WHERE  e.page = 'NextSong';
                        """)


song_table_insert     = ("""INSERT INTO DIMENSION_SCHEMAS.SONGS (song_id, title, artist_id, year, duration)
                            SELECT DISTINCT s.song_id, s.title, s.artist_id, s.year, s.duration
                            FROM   STAGING_SCHEMAS.STAGING_SONGS AS s;
                        """)


artist_table_insert   = ("""INSERT INTO DIMENSION_SCHEMAS.ARTISTS (artist_id, artist_name, artist_location, 
                                                 artist_latitude, artist_longitude)
                            SELECT DISTINCT s.artist_id, s.artist_name, s.artist_location, 
                                            s.artist_latitude, s.artist_longitude
                            FROM   STAGING_SCHEMAS.STAGING_SONGS AS s;
                        """)


time_table_insert     = ("""INSERT INTO DIMENSION_SCHEMAS.TIME (start_time, hour, day, week, month, year, weekday)
                            SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS                                                start_time, EXTRACT(hour FROM start_time), EXTRACT(day FROM start_time),
                                   EXTRACT(week FROM start_time), EXTRACT(month FROM start_time),
                                   EXTRACT(year FROM start_time), EXTRACT(week FROM start_time)
                            FROM   STAGING_SCHEMAS.STAGING_EVENTS AS e
                            WHERE  e.page = 'NextSong';
                        """)

# QUERY LISTS

create_tables_queries  = [staging_events_table_create, staging_songs_table_create, songplay_table_create,                                 user_table_create, song_table_create, artist_table_create, time_table_create]
create_schemas_queries = [fact_schema, dimension_schema, staging_schema]
drop_schemas_queries   = [fact_schema_drop, dimension_schema_drop, staging_schema_drop]
copy_table_queries     = [staging_events_copy, staging_songs_copy]
insert_table_queries   = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,                               time_table_insert]
