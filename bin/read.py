#!/usr/bin/env python3
from bin.settings import settings
import sqlite3
import ujson
import os
import sqlparse
import logging

def stream_output(cursor, query, start_response):
    first = True
    try:
        for row in cursor.execute(query.decode('utf-8')):
            if first:
                start_response('200 OK', [('Content-Type','application/json')])
                yield b"["
            if not first:
                yield b","
            output = dict((cursor.description[i][0], value) for i, value in enumerate(row))
            yield ujson.dumps(output).encode('utf-8')
            first = False
        yield b"]"
    except Exception as e:
        start_response('500 Server Error', [('Content-Type', 'text/plain')])
        yield [b'500 Server Error: %s' % str(e).encode('utf-8')]
    cursor.close()
    cursor.connection.close()

def read(env, start_response):
    try:
        database = os.path.split(env.get('PATH_INFO'))[-1] # get database id from host/request path
        database = os.path.join(settings['LOCAL_DATA'], "{name}.sqlite".format(name=database))
        connection = sqlite3.connect(database)
        cur = connection.cursor()

        query = env.get('wsgi.input').read()
        logging.info('Servicing request: {query}'.format(query=query))
        # if the user sent more than one query, or the query is not a SELECT, raise an exception.
        if len(sqlparse.split(query)) != 1 or sqlparse.parse(query)[0].get_type() != 'SELECT':
            raise Exception('Exactly one SELECT query per request, please.')
        return stream_output(cur, query, start_response)

    except Exception as e:
        start_response('500 Server Error', [('Content-Type', 'text/plain')])
        return [b'500 Server Error: %s' % str(e).encode('utf-8')]