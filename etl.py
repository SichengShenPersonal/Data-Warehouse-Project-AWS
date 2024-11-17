import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """
    Copies data from source systems into staging tables in the database.

    Parameters:
    cur (cursor): The database cursor object for executing queries.
    conn (connection): The database connection object for committing transactions.

    This function iterates through the list of queries in `copy_table_queries`,
    executing each to load data from external sources into staging tables.
    Commits each query after execution to ensure the data is properly staged.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
    Inserts data from staging tables into the star schema tables in the database.

    Parameters:
    cur (cursor): The database cursor object for executing queries.
    conn (connection): The database connection object for committing transactions.

    This function iterates through the list of queries in `insert_table_queries`,
    executing each to transform and load data into the star schema tables.
    Commits each query after execution to finalize the data insertion.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    Establishes a connection to the database, loads data into staging tables,
    and inserts transformed data into the star schema tables.

    The function reads configuration details from the `dwh.cfg` file to connect
    to the database, executes functions to load staging tables and insert
    data into star schema tables, and closes the connection upon completion.
    """
    # Load configuration settings for the database connection
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to the database using parameters from the config file
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
    )
    cur = conn.cursor()

    # Load data into staging tables and then insert into star schema tables
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # Close the database connection
    conn.close()

if __name__ == "__main__":
    main()