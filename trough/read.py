#!/usr/bin/env python3
from trough.settings import settings
import sqlite3
import ujson
import os
import sqlparse
import logging

class ReadServer:
    def __init__(self, start_response):
        self.start_response = start_response

    def read(self, segment_path, query):
        logging.info('Servicing request: {query}'.format(query=query))
        # if the user sent more than one query, or the query is not a SELECT, raise an exception.
        if len(sqlparse.split(query)) != 1 or sqlparse.parse(query)[0].get_type() != 'SELECT':
            raise Exception('Exactly one SELECT query per request, please.')
        connection = sqlite3.connect(segment_path)
        cursor = connection.cursor()
        first = True
        try:
            self.start_response('200 OK', [('Content-Type','application/json')])
            yield b"["
            for row in cursor.execute(query.decode('utf-8')):
                if not first:
                    yield b",\n"
                output = dict((cursor.description[i][0], value) for i, value in enumerate(row))
                yield ujson.dumps(output).encode('utf-8')
                first = False
            yield b"]"
        finally:
            # close the cursor 'finally', in case there is an Exception.
            cursor.close()
            cursor.connection.close()

# uwsgi endpoint
def application(env, start_response):
    try:
        segment_name = env.get('HTTP_HOST', "").split(".")[0] # get database id from host/request path
        segment_path = os.path.join(settings['LOCAL_DATA'], "{name}.sqlite".format(name=segment_name))
        query = env.get('wsgi.input').read()
        return ReadServer(start_response).read(segment_path, query)
    except Exception as e:
        start_response('500 Server Error', [('Content-Type', 'text/plain')])
        return [b'500 Server Error: %s' % str(e).encode('utf-8')]
