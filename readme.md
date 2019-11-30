# Data modeling with Postgres project

The project consists of modeling and ETL pipeline in Python. The pipeline manages flat files from a streaming app and puts them inside a relational database (Postgres) in a start schema (dimensional) model.

## Running
To run the project, first you must first get the dataset downloaded. You can find it here: http://millionsongdataset.com/

Now download and install the Python dependencies - more information in the Dependencies topic below.

You can set up the Postgres server locally. The create_tables.py file is the first one you have to run. It'll create the db and tables required for the project:

    python3 create_tables.py

This makes use of the 'sql_queries.py' file that has stored the queries to be run against the db.

Then, run the etl.py file

    python3 etl.py

This will properly pick up the files on the data/log_data and data/song_data folders to parse and insert into the db.

## Dependencies
Below are the python modules needed to be installed before running the project:
pandas
psycopg2

## Dimesional model
The model is a start schema, with one fact table (songplays) containing other four dimensional tables (time, artists, songs, users)
