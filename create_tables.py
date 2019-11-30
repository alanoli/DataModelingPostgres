import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    # connect to default database
    conn = psycopg2.connect("host=localhost dbname=postgres user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()
    
    # connect to sparkify database
    conn = psycopg2.connect("host=localhost dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn

def drop_tables(cur, conn):
    for query in drop_table_queries:
        try:
            cur.execute(query)
        except Exception as e:
            pass
    conn.commit()

def create_tables(cur, conn):
    for query in create_table_queries:
        try:
            cur.execute(query)
        except Exception as e:
            print(e)
            raise
    conn.commit()

def main():
    cur, conn = create_database()
    
    try:
        drop_tables(cur, conn)
    except Exception as e:
        print(e)
        
    try:
        create_tables(cur, conn)
        print('Tables created sucessfully')
    except Exception as e:
        print(e)
        print('Aborting table creation')
    
    conn.close()

if __name__ == "__main__":
    main()