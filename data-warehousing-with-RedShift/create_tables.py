import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Drop all the old table to avoid conflict or stale data
    
    Arg: 
        cur - cursor of the connecting database
        conn - connection to the database
        
    Return:
        None - All previous tables are dropped
        
    '''
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            raise e


def create_tables(cur, conn):
    '''
    Create staging, dimension, and fact tables
    
    Arg: 
        cur - cursor of the connecting database
        conn - connection to the database
        
    Return:
        None - All tables are created in Redshift
        
    '''
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            raise e
            


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()