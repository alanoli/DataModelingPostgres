# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id SERIAL PRIMARY KEY NOT NULL
    ,start_time timestamp REFERENCES time(start_time)
    ,user_id int REFERENCES users(user_id) NOT NULL
    ,level varchar
    ,song_id varchar REFERENCES songs(song_id)
    ,artist_id varchar REFERENCES artists(artist_id)
    ,session_id int NOT NULL
    ,location varchar
    ,user_agent_id varchar
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id int PRIMARY KEY NOT NULL
    ,first_name varchar NOT NULL
    ,last_name varchar
    ,gender varchar(1)
    ,level varchar
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    song_id varchar PRIMARY KEY NOT NULL
    ,title varchar
    ,artist_id varchar
    ,year int
    ,duration numeric
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id varchar PRIMARY KEY NOT NULL
    ,name varchar NOT NULL
    ,location varchar
    ,latitude numeric
    ,longitude numeric
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time timestamp PRIMARY KEY NOT NULL
    ,hour int NOT NULL
    ,day int NOT NULL
    ,week int NOT NULL
    ,month int NOT NULL
    ,year int NOT NULL
    ,weekday varchar NOT NULL
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays
(
    start_time
    ,user_id
    ,level
    ,song_id
    ,artist_id
    ,session_id
    ,location
    ,user_agent_id
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (songplay_id) DO UPDATE SET level=EXCLUDED.level;
""")

user_table_insert = ("""
INSERT INTO users
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO NOTHING;
""")

song_table_insert = ("""
INSERT INTO songs
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING;
""")

time_table_insert = ("""
INSERT INTO time
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT
    songs.song_id
    ,artists.artist_id
FROM
    songs
    JOIN artists ON artists.artist_id = songs.artist_id
WHERE
    songs.title = %s
    AND artists.name = %s
    AND songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [artist_table_create, song_table_create, time_table_create, user_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
