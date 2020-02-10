# db.py
# Rohan Weeden
# Created: May 16, 2018

# Module for proc_lib database functions

import time

import psycopg2

from .config import get_config
from .logger import log


def get_db_connection(s, tries=0):
    connection_string =\
        "host='" + get_config(s, 'host') + "' " + \
        "dbname='" + get_config(s, 'db') + "' " + \
        "user='" + get_config(s, 'user') + "' " + \
        "password='" + get_config(s, 'pass') + "'"
    log.info("Connected to db: {0}".format(get_config(s, 'host')))
    try:
        conn = psycopg2.connect(connection_string)
    except Exception as e:
        if (tries > 4):
            log.exception('DB connection problem: '+str(e))
            raise
        else:
            log.warning("Problem connecting to DB: "+str(e))
            log.info("Retrying in {0} seconds...".format(30*(tries+1)))
            time.sleep(30*(tries+1))
            return get_db_connection(s, tries=tries+1)

    return conn


def query_database(conn, query, params=None, commit=False, returning=False):
    """Query a database.

    Takes a database connection, query represented by a string, query
    parameters represented by a list, and a boolean representing
    whether the query makes changes to the database, and returns the
    results of the query if the boolean is False.
    """
    if params is None:
        params = []
    cur = conn.cursor()
    cur.execute(query, params)

    ret = None
    if commit:
        conn.commit()
        if returning:
            ret = cur.fetchall()
        else:
            ret = cur.rowcount

    else:
        conn.rollback()
        ret = cur.fetchall()
    cur.close()
    return ret


def get_db_config(conn, key):
    r = query_database(
        conn, "SELECT value FROM config WHERE key = %s", (key,))
    if r and r[0] and r[0][0]:
        return r[0][0]
    else:
        return None


def get_user_email(user_id, conn):
    recs = query_database(conn, "SELECT email, username, wants_email FROM users WHERE id = %s", (user_id,))
    if recs and len(recs) > 0 and len(recs[0]) > 0:
        return recs[0][0], recs[0][1], recs[0][2]
    else:
        return None, None


def get_user_info(cfg, conn):
    return query_database(
        conn,
        '''
            SELECT users.username, users.email, users.wants_email, subscriptions.name, processes.name
            FROM local_queue
                LEFT JOIN subscriptions ON local_queue.sub_id = subscriptions.id
                JOIN users ON local_queue.user_id = users.id
                JOIN processes ON local_queue.process_id = processes.id
            WHERE local_queue.id = %s
        ''',
        params=(cfg['id'],),
    )[0]
