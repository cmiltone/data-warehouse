import configparser
import psycopg2
from sql_queries import analytics_queries


def do_analytics(cur, conn):
    """
    method that runs queries to get analytics of
    hours and song plays
    """
    query_titles = [
        "Five most played songs",
        "Five highest usage time of day"
    ]
    indx = 0
    for query in analytics_queries:
        cur.execute(query)
        conn.commit()
        print("{}:\n".format(query_titles[indx]))
        for row in cur.fetchall():
            print("\t{} ({})".format(row[0], row[1]))
        print("\n\n")
        indx += 1


def main():
    """
    entrypoint function that is executed when this
    script is ran and calls the do_analytics method
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    do_analytics(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()