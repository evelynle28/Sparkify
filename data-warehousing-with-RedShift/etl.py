import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Copy data from json files in the source database (S3)
     into the staging tables to avoid affecting the integrity
     of the source database(s)
    
    Arg: 
        cur - cursor of the connecting database
        conn - connection to the database
        
    Return:
        None - All data are copied into the staging_songs 
                and staging_events tables
        
    '''
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            raise e


def insert_tables(cur, conn):
    '''
    Load data from the staging tables into the dimension
     and fact tables created in the target database
    
    Arg: 
        cur - cursor of the connecting database
        conn - connection to the database
        
    Return:
        None - All data are loaded to the corresponding
                dim and fact table in the target database
        
    '''
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            raise e


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    
    # Connect to the Redshift cluster - target database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Load from S3 (source database) -> Staging Tables
    load_staging_tables(cur, conn)
    
    # Load from Staging Tables -> Redshift Cluster (target database)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()