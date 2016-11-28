#!/usr/bin/env python3
from bin.settings import settings
import sqlite3
import ujson
import os
import sqlparse
import logging

def stream_output(cursor, query):
    yield b"["
    first = True
    for row in cursor.execute(query.decode('utf-8')):
        if not first:
            yield b","
        output = dict((cursor.description[i][0], value) for i, value in enumerate(row))
        yield ujson.dumps(output).encode('utf-8')
        first = False
    yield b"]"
    cursor.close()
    cursor.connection.close()

def read(env, start_response):
    try:
        database = os.path.split(env.get('PATH_INFO'))[-1] # get database id from host/request path
        database = os.path.join(settings['DATA_ROOT'], "{name}.sqlite".format(name=database))
        connection = sqlite3.connect(database)
        cur = connection.cursor()

        query = env.get('wsgi.input').read()
        logging.info('Servicing request: {query}'.format(query=query))
        if len(sqlparse.split(query)) != 1:
            raise Exception('Exactly one SELECT query at a time.')
        parsed = sqlparse.parse(query)

        start_response('200 OK', [('Content-Type','application/json')])
        return stream_output(cur, query)

    except Exception as e:
        start_response('500 Server Error', [('Content-Type', 'text/plain')])
        return [b'500 Server Error: %s' % str(e).encode('utf-8')]