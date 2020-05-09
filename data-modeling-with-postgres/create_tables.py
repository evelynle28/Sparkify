import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """

  # Connect to default database
    try:
        connection = psycopg2.connect(database="postgres",
                                user="evelyn",
                                password="")
    except Exception as e:
        print("Failed to connect default database:",  e)
    else:
        connection.set_session(autocommit=True)
        cur = connection.cursor()

        # create sparkify database with UTF8 encoding
        cur.execute("DROP DATABASE IF EXISTS sparkify_db")
        cur.execute("CREATE DATABASE sparkify_db \
                      WITH ENCODING 'utf8' \
                      TEMPLATE template0")
    finally:
      # Close the connection to the default database
        connection.close()

    # connect to sparkify database
    try:
        connection = psycopg2.connect(database="sparkify_db",
                                  user="evelyn",
                                  password="")
    except Exception as e:
        print ("Failed to connect to sparkify database:", e)

    else:
      connection.set_session(autocommit=True)
      cur = connection.cursor()

    return cur, connection

def create_tables(cur, conn):
    """
      Creates each table using `create_table_queries` list.
    """
    for create_query in create_table_queries:
        try:
            cur.execute(create_query)
        except Exception as e:
            print("Failed to create table: ", e)

def drop_tables(cur, conn):
    """
      Drop each table using `drop_table_queries` list
    """
    for drop_query in drop_table_queries:
        try:
            cur.execute(drop_query)
        except Exception as e:
            print("Failed to drop table: ", e)

def main():
    """
      - Drops (if exists) and Creates the sparkify database.

      - Establishes connection with the sparkify database and gets
      cursor to it.

      - Drops all the tables.

      - Creates all tables needed.

      - Finally, closes the connection.
    """

    cur, conn = create_database()
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
      main()


