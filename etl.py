import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    ''' 
    copy the data from datasets and store them into staging tables
    it contains two parameters:conn and cur as following:
    conn: it is the connecter between database and python
    cur: it is object that perform database operations in python file
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    '''
    insert the data into tables from staging tables
    it contains two parameters:conn and cur as following:
    conn: it is the connecter between database and python
    cur: it is object that perform database operations in python file
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    ### create configparser that read configuration data from a file "dwh.cfg" 
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    ### create a connection to a database that their details are stored in 'dwh.cfg' file
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    ### copy the data from datasets to staging then tables
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()