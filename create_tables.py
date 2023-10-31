import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    method to run queries to delete tables
    inacase they exist in preparation to insert
    data 
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    method to run queries to create tables
    based on defined schemas
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    entrypoint method that is executed when this script
    is run and calls the drop_tabes
    and create_tables functions
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()