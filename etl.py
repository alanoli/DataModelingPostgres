import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Gets information from a song data file (artist and song) and inserts into database tables"""
    df = pd.read_json(filepath, typ='series')

    artist_data = []
    artist_data.append(df['artist_id'])
    artist_data.append(df['artist_name'])
    artist_data.append(df['artist_location'])
    artist_data.append(df['artist_latitude'])
    artist_data.append(df['artist_longitude'])
    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e:
        print("Not possible to insert into artists. " + str(e))
    
    song_data = []
    song_data.append(df['song_id'])
    song_data.append(df['title'])
    song_data.append(df['artist_id'])
    song_data.append(df['year'])
    song_data.append(df['duration'])
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e:
        print("Not possible to insert into songs. " + str(e))

def process_log_file(cur, filepath):
    """Reads from a log file and inserts data in songplays fact table"""
    df = pd.read_json(filepath, lines=True, encoding='raw_unicode_escape')
    df = df[df['page'] == 'NextSong']
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    time_data = [
        df.ts
        ,df.ts.dt.hour
        ,df.ts.dt.day
        ,df.ts.dt.weekofyear
        ,df.ts.dt.month
        ,df.ts.dt.year
        ,df.ts.dt.weekday
    ]
    
    column_labels = ['timestamp', 'hour', 'day', 'weekofyear', 'month', 'year', 'weekday']
    ts_dict = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(ts_dict)

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except Exception as e:
            print("Not possible to insert into time. " + str(e))

    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for index, row in df.iterrows():
        
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
        except Exception as e:
            print(e)
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data = (str(row.ts), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent.replace('\"', ''))
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except Exception as e:
            print(e)


def process_data(cur, conn, filepath, func):
    """Reads from directory to retrieve filenames for processing by 'func' function"""
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """This connects to database and processes file data"""
    conn = psycopg2.connect("host=localhost dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
