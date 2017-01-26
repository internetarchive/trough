#!/usr/bin/env python3
from bin.settings import settings
import sqlite3
import ujson
import os
import sqlparse
import logging

class WriteServer:
    def __init__(segment_name):
        self.segment_name = segment_name
        self.segment_path = os.path.join(settings['LOCAL_DATA'], "{name}.sqlite".format(name=database))
        self.connection = sqlite3.connect(database)
        self.cursor = self.connection.cursor()

    def write(self, query, start_response):
        logging.info('Servicing request: {query}'.format(query=query))
        # if the user sent more than one query, or the query is not a SELECT, raise an exception.
        for q in sqlparse.parse(q).get_type() not in ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER']:
            raise Exception('This server only accepts "Write" queries, that is "INSERT", "UPDATE", "DELETE", "CREATE" or "ALTER".')
        output = self.cursor.execute(query.decode('utf-8'))
        self.cursor.close()
        self.cursor.connection.close()
        return str(output).encode('utf-8')

def application(env):
    try:
        segment_name = env.get('HTTP_HOST', "").split(".")[0] # get database id from host/request path
        query = env.get('wsgi.input').read()
        return WriteServer(segment_name).write(query, start_response)
    except Exception as e:
        start_response('500 Server Error', [('Content-Type', 'text/plain')])
        return [b'500 Server Error: %s' % str(e).encode('utf-8')]