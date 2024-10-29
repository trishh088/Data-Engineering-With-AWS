import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# VARIABLES NEEDED FOR THE SCRIPT
REGION='us-west-2' # as thats where the files are and we want to save on cost of moving it from west to east region
LOG_DATA_S3 = config.get('S3', 'LOG_DATA')
LOG_JSONPATH_S3 = config.get('S3', 'LOG_JSONPATH')
SONG_DATA_S3 = config.get('S3', 'SONG_DATA')
IAM_ROLE_ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE;"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE;"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE;"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE;"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE;"

# CREATE TABLES
# reading the raw json events file data 
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events 
        (
            artist VARCHAR,
            auth VARCHAR,
            firstName VARCHAR,
            gender CHAR,
            itemInSession INT,
            lastName VARCHAR,
            length FLOAT,
            level VARCHAR,
            location VARCHAR,
            method CHAR(4),
            page VARCHAR,
            registration FLOAT,
            sessionId INT,
            song VARCHAR,
            status INT,
            ts BIGINT,
            userAgent VARCHAR,
            userId INT
        )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
        (
            num_songs INT,
            artist_id VARCHAR,
            artist_latitude FLOAT,
            artist_longitude FLOAT,
            artist_location VARCHAR,
            artist_name VARCHAR,
            song_id VARCHAR,
            title VARCHAR,
            duration FLOAT,
            year INT
        )
                              
""")

# DIM Table
# Here we are continuing the logic of carrying of using user_id as distkey 
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
user_id INT PRIMARY KEY SORTKEY DISTKEY,
                     first_name VARCHAR NOT NULL, 
                     last_name VARCHAR NOT NULL,
                     gender CHAR NOT NULL,
                     level VARCHAR NOT NULL
                     )
""")

# DIM Tables
# not using distkey on artist id here as a user might not 
# always listen to one artists in one go or always listen to a artits's album
artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists 
        (
            artist_id VARCHAR PRIMARY KEY SORTKEY,
            name VARCHAR NOT NULL,
            location VARCHAR,
            latitude FLOAT,
            longitude FLOAT        
                           
        )
""")
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
song_id VARCHAR PRIMARY KEY SORTKEY,
title VARCHAR NOT NULL,
artist_id VARCHAR NOT NULL REFERENCES artists(artist_id),
year INT NOT NULL,
duration FLOAT NOT NULL      
); 
""")


time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time TIMESTAMP PRIMARY KEY SORTKEY,
        hour INT NOT NULL,
        day INT NOT NULL,
        week INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        weekday INT NOT NULL
    )
""")

# Fact table
# here we are adding user as DISTKEY as it will represent in each cluster the user's 
# play history and sorting it by the user's sessionsongplay_table_create = (
songplay_table_create = (
    """CREATE TABLE IF NOT EXISTS songplays
        (
            songplay_id INT IDENTITY(0,1) PRIMARY KEY,
            start_time TIMESTAMP NOT NULL,
            user_id INT NOT NULL DISTKEY REFERENCES users(user_id), 
            level VARCHAR NOT NULL, 
            song_id VARCHAR NOT NULL REFERENCES songs(song_id), 
            artist_id VARCHAR NOT NULL REFERENCES artists(artist_id), 
            session_id INT NOT NULL SORTKEY, 
            location VARCHAR NOT NULL, 
            user_agent VARCHAR NOT NULL
        )
    """
)

# STAGING TABLES

# staging_events_copy = ("""
# COPY staging_events FROM {}
# CREDENTIALS  'aws_iam_role={}'
# FORMAT AS JSON {}
# REGION {};
# """).format(LOG_DATA_S3,IAM_ROLE_ARN, LOG_JSONPATH_S3,REGION )

staging_events_copy = ("""
COPY staging_events FROM '{}'
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON '{}'
REGION '{}'
TIMEFORMAT 'epochmillisecs';
                       
""").format(LOG_DATA_S3, IAM_ROLE_ARN, LOG_JSONPATH_S3, REGION)


staging_songs_copy = ("""
COPY staging_songs FROM '{}'
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 'auto'
REGION '{}';
""").format(SONG_DATA_S3,IAM_ROLE_ARN, REGION )

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' AS start_time,
        se.userId as user_id,
        se.level,
        ss.song_id,
        ss.artist_id, 
        se.sessionId as session_id, 
        se.location, 
        se.userAgent as user_agent
FROM staging_events se JOIN 
     staging_songs ss ON ss.artist_name = se.artist 
                         AND ss.title=se.song
    WHERE se.page='NextSong';
       
""")
# here using distinct for the combination of users as if a user converts from free to paid, I would like that captured as well
# since this isn't a incremental load or an update we are not considering to check or adding a where clause to check 
# and see if the user already exists in the table or not
user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT 
        DISTINCT 
        se.userId as user_id,
        se.firstName as first_name,
        se.lastName as last_name,
        se.gender,
        se.level           
FROM staging_events se
WHERE se.userId is not NULL ; -- added this as other wise the insert would fail  
                     """)

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT ss.song_id,
       ss.title,
       ss.artist_id,
       ss.year,
       ss.duration             
FROM staging_songs ss
WHERE ss.song_id IS NOT NULL;
                     """)
 
artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT ss.artist_id,
       ss.artist_name as name,
       ss.artist_location as location,
       ss.artist_latitude as latitude,
       ss.artist_longitude as longitude                
FROM staging_songs ss
WHERE ss.artist_id IS NOT NULL;
                       """)

# postgre and redshift allow to use a column created earlier in select to be used in place for other columns rather than 
# doing the extract to_timestamp calculation for evert row
time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT                  
TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' AS start_time, 
EXTRACT(hour from start_time) as hour,
EXTRACT(day from start_time) as day,
EXTRACT(week from start_time) as week,
EXTRACT(month from start_time) as month,
EXTRACT(year from start_time) as year,
EXTRACT(dayofweek from start_time) as weekday
FROM staging_events;               
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
# drop_table_queries = [
#     songplay_table_drop, 
#     user_table_drop, 
#     artist_table_drop, 
#     song_table_drop, 
#     time_table_drop,
#     staging_events_table_drop, 
#     staging_songs_table_drop
# ]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]



# songplay_table_create = ("""
#     CREATE TABLE IF NOT EXISTS songplays
#         (
#             artist VARCHAR,
#             song VARCHAR,
#             song_id VARCHAR,
#             userAgent VARCHAR,
#             ts BIGINT, --(timestamp when the song was played)
#             page VARCHAR,
#             length FLOAT, -- (song payed by user length)
#             duration FLOAT, -- (song's actual length)
#             artist_id VARCHAR

#         )
# """)

# user_table_create = ("""
# CREATE TABLE IF NOT EXISTS users 
# (
#                      userId INT,
#                      firstName VARCHAR,
#                      lastName VARCHAR,
#                      gender CHAR,
#                      level VARCHAR,
#                      location VARCHAR,
#                      page VARCHAR,
#                      registration FLOAT,
#                      sessionid INT,
#                      userAgent VARCHAR,
#                      auth VARCHAR,
#                      song VARCHAR #song_played
#                      length FLOAT (song payed by user length)
# )
# """)

# song_table_create = ("""
# CREATE TABLE IF NOT EXISTS songs
# (
# song_id VARCHAR,
# title VARCHAR,
#                      duration FLOAT,
#                      year INT,
# artist_name VARCHAR,
# artist_id VARCHAR                     
                    
# )
# """)

# artist_table_create = ("""
#     CREATE TABLE IF NOT EXISTS artists 
#         (
#             artist_sr_no IDENTITY(0,1) PRIMARY KEY,
#             artist_ID VARCHAR,
#             artist_latitude VARCHAR,
#             artist_longitude FLOAT,
#             artist_location VARCHAR,
#             artist_name VARCHAR,
#             song_id VARCHAR,
#             title VARCHAR
#             duration FLOAT,
#             year INT           
                           
#         )
# """)

# time_table_create = ("""
#     CREATE TABLE IF NOT EXISTS time
#     (
#             time_sr_no IDENTITY(0,1) PRIMARY KEY,
#             user_id INT,
#             ts BIGINT,
#             song_id VARCHAR,
#             title VARCHAR
#             duration FLOAT,
#             year INT,
#             registration FLOAT
#     )
# """)
