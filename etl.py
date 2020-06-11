import configparser
import psycopg2
import logging
from sql_queries import copy_table_queries, insert_table_queries

FORMAT = "%(asctime)s %(name)s %(levelname)s %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)
logger = logging.getLogger(__name__)

def load_staging_tables(cur, conn):
    """
    Load data into staging tables by means of COPY queries

    Parameters
    ----------
    cur : psycopg2 Cursor
    conn : psycopg2 Connection

    """
    try:

        for query in copy_table_queries:
            logger.info(f"Executing query {query}")
            cur.execute(query)
            conn.commit()

    except psycopg2.Error:
        logger.exception("Issue while loading data into staging tables")
    except Exception:
        logger.exception(f"Issue while preparing query {query}")


def insert_tables(cur, conn):
    """
    Execute INSERT queries to load data from staging tables to redshift database

    Parameters
    ----------
    cur : psycopg2 Cursor
    conn : psycopg2 Connection

    """
    try:
        for query in insert_table_queries:
            logger.info(f"Executing query {query}")
            cur.execute(query)
            conn.commit()

    except psycopg2.Error:
        logger.exception("Issue while inserting data into redshift database")


def main():
    """
    Connect to Redshift database, load data into staging tables and then execute insert queries

    """
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()
    load_staging_tables(cur, conn)
    logger.info("Staging tables loaded successfully")
    insert_tables(cur, conn)
    logger.info("Final tables loaded successfully")

    conn.close()


if __name__ == "__main__":
    main()
