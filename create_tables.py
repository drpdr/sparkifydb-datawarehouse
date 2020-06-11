import configparser
import psycopg2
import logging
from sql_queries import create_table_queries, drop_table_queries


FORMAT = "%(asctime)s %(name)s %(levelname)s %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)
logger = logging.getLogger(__name__)


def drop_tables(cur, conn):
    """Drop all the tables in sparkifydb if they exist
    
    Parameters
    ----------
    cur : psycopg2 Cursor
    conn : psycopg2 Connection
    
    """
    try:
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
    except psycopg2.Error:
        logger.exception("Issue while dropping the tables")


def create_tables(cur, conn):
    """Create all the tables in sparkifydb if they do not exist
    
    Parameters
    ----------
    cur : psycopg2 Cursor
    conn : psycopg2 Connection
    
    """
    try:
        for query in create_table_queries:
            cur.execute(query)
            logger.info(f"Executing query {query}")
            conn.commit()
    except psycopg2.Error:
        logger.exception("Issue while creating the tables")


def main():
    """Connect to the Redshift cluster reading the configuration file (dwh.cfg), drop the tables, and create them"""

    try:
        config = configparser.ConfigParser()
        config.read("dwh.cfg")

        conn = psycopg2.connect(
            "host={} dbname={} user={} password={} port={}".format(
                *config["CLUSTER"].values()
            )
        )
        cur = conn.cursor()

        drop_tables(cur, conn)
        create_tables(cur, conn)

        conn.close()
    except psycopg2.Error:
        logger.exception("Issue while connecting to the cluster")
    except Exception:
        logger.exception("Issue while reading the configuration file")


if __name__ == "__main__":
    main()
