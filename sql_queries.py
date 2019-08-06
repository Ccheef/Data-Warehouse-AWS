import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table(artist varchar, auth varchar, firstName varchar, gender varchar, itemInSession int,lastName varchar, length DOUBLE PRECISION, level varchar, location varchar,
method varchar, page varchar, registration DOUBLE PRECISION, sessionId INTEGER, song varchar, status INTEGER, ts bigint, userAgent varchar, userId INTEGER)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table(num_songs int, artist_id varchar NOT NULL, artist_latitude DOUBLE PRECISION, artist_longitude DOUBLE PRECISION,  artist_location varchar, artist_name varchar, song_id varchar, title varchar, duration DOUBLE PRECISION, year INTEGER)
""")
#take song_id as distkey to eliminate the shuffling from joining on songs, take start_time as sortkey to sort out the time trend for possible analytics use
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table(songplay_id INTEGER IDENTITY(0, 1) PRIMARY KEY, start_time timestamp NOT NULL sortkey, user_id INTEGER, level varchar, song_id varchar distkey, artist_id varchar, session_id int, location varchar, user_agent varchar)
""")

#userId from events-log data has null value, so not set user_id as primary key here
user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table(user_id int sortkey, first_name varchar, last_name varchar, gender varchar, level varchar) diststyle all;
""")
#set song_id as sortkey for better efficiency on possible aggregation on song_id. Similar to artist_id and start_time
song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table(song_id varchar PRIMARY KEY distkey sortkey, title varchar, artist_id varchar NOT NULL, year INTEGER, duration DOUBLE PRECISION)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table(artist_id varchar PRIMARY KEY sortkey, name varchar, location varchar, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION) 
diststyle all;
""")
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table(start_time timestamp PRIMARY KEY sortkey, hour INTEGER NOT NULL, day INTEGER NOT NULL, week INTEGER NOT NULL, month INTEGER NOT NULL, year INTEGER NOT NULL, weekday INTEGER NOT NULL) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events_table
from {}
credentials 'aws_iam_role={}'
json {}
region 'us-west-2';
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs_table from {}
credentials 'aws_iam_role={}' 
json 'auto'
region 'us-west-2';
""").format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'))

# FINAL TABLES
#choose song_id, artist_id from songs dataset. Others from event dataset
songplay_table_insert = ("""
INSERT INTO songplay_table(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
(SELECT timestamp 'epoch' + ts * interval '1 second' AS start_time, userId, level, song_id, artist_id, sessionId, location, userAgent 
FROM staging_events_table, staging_songs_table
WHERE page = 'NextSong' AND title = song AND artist_name = artist) 
""")
# Select 'Distinct' to avoid duplicated IDs, same for other tables
user_table_insert = ("""
INSERT INTO user_table(user_id, first_name, last_name, gender, level)
(SELECT DISTINCT userId,firstName,lastName,gender,level FROM staging_events_table);
""")

song_table_insert = ("""
INSERT INTO song_table(song_id, title, artist_id, year, duration)
(SELECT DISTINCT song_id, title, artist_id, year,duration FROM staging_songs_table);
""")

artist_table_insert = ("""
INSERT INTO artist_table(artist_id, name, location, latitude, longitude)
(SELECT DISTINCT artist_id,artist_name,artist_location,artist_latitude,artist_longitude FROM staging_songs_table)
""")
#calculate the elements from the start_time timestamp
time_table_insert = ("""
INSERT INTO time_table(start_time, hour, day, week, month, year, weekday)
(SELECT DISTINCT timestamp 'epoch' + ts * interval '1 second' AS start_time , extract(hour from start_time), extract(day from start_time), extract(week from start_time),
extract(month from start_time), extract(year from start_time), extract(weekday from start_time) FROM staging_events_table)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
