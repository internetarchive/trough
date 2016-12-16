#!/usr/bin/env python3
from bin.settings import settings
import sqlite3
import ujson
import os
import sqlparse
import logging

def write(env, start_response):
    try:
        database = os.path.split(env.get('PATH_INFO'))[-1] # get database id from host/request path
        database = os.path.join(settings['LOCAL_DATA'], "{name}.sqlite".format(name=database))
        connection = sqlite3.connect(database)
        cur = connection.cursor()

        query = env.get('wsgi.input').read()
        logging.info('Servicing request: {query}'.format(query=query))
        # if the user sent more than one query, or the query is not a SELECT, raise an exception.
        for q in sqlparse.parse(q).get_type() not in ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER']:
            raise Exception('This server only accepts "Write" queries, that is "INSERT", "UPDATE", "DELETE", "CREATE" and "ALTER".')
        output = cursor.execute(query.decode('utf-8'))
        cursor.close()
        cursor.connection.close()
        return str(output).encode('utf-8')

    except Exception as e:
        start_response('500 Server Error', [('Content-Type', 'text/plain')])
        return [b'500 Server Error: %s' % str(e).encode('utf-8')]