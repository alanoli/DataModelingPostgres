import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert artist record
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
    
    # insert song record
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
    # open log file
    df = pd.read_json(filepath, lines=True, encoding='raw_unicode_escape')

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
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

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        # print(row.song)
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
        except Exception as e:
            print(e)
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (str(row.ts), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent.replace('\"', ''))
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except Exception as e:
            print(e)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=localhost dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()