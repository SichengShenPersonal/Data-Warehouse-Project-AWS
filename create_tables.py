import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    Executes SQL queries to drop existing tables in the database.

    Parameters:
    cur (cursor): The database cursor object for executing queries.
    conn (connection): The database connection object for committing transactions.

    This function iterates through the list of queries defined in `drop_table_queries`
    and drops each table to clear out any existing schema. Commits each query after execution.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Executes SQL queries to create new tables in the database.

    Parameters:
    cur (cursor): The database cursor object for executing queries.
    conn (connection): The database connection object for committing transactions.

    This function iterates through the list of queries defined in `create_table_queries`
    and creates each table as specified. Commits each query after execution.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    Establishes a connection to the Redshift database, drops existing tables,
    and creates new ones based on the defined schema.

    The function reads configuration details from the `dwh.cfg` file to connect
    to the database, executes table drop and creation functions, and closes the connection.
    """
    # Load configuration settings for the database connection
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to the database using parameters from the config file
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
    )
    cur = conn.cursor()

    # Drop any existing tables and recreate them with the defined schema
    drop_tables(cur, conn)
    create_tables(cur, conn)

    # Close the database connection
    conn.close()

if __name__ == "__main__":
    main()