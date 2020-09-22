import sqlite3
from sqlite3 import Error

from sql import sql_create_links_table


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
        return conn
    except Error as e:
        print(e)


def create_table(conn, create_table_sql):
    c = conn.cursor()
    c.execute(create_table_sql)


def create_link(conn, link_data):
    sql = '''
        INSERT INTO links(path, domain, protocol)
        VALUES(?, ?, ?)
    '''
    c = conn.cursor()
    c.execute(sql, link_data)
    conn.commit()
    return c.lastrowid


def create_word_row(conn, params):
    print(params)
    sql = f"""
        INSERT INTO rating_for_page_{params[0]} (word, rating)
        VALUES(?, ?)
    """
    c = conn.cursor()
    c.execute(sql, (params[1], params[2]))
    conn.commit()
    return c.lastrowid


def select_all_links(conn):
    c = conn.cursor()
    c.execute('SELECT * FROM links')
    rows = c.fetchall()
    return None if len(rows) == 0 else rows


def select_ratings(conn, i):
    c = conn.cursor()
    try:
        c.execute(f'SELECT * FROM rating_for_page_{i}')
    except sqlite3.OperationalError:
        return None

    rows = c.fetchall()
    return rows