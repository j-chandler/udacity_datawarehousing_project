import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''Executes all DROP TABLE sql statements for the database connected to'''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''Executes all CREATE TABLE for the database connected to'''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''Reads the config, drops old tables and then creates new ones'''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("Dropping")
    drop_tables(cur, conn)
    
    print("Creating")
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()