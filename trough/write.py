#!/usr/bin/env python3
from trough.settings import settings
import sqlite3
import ujson
import os
import sqlparse
import logging

class WriteServer:
    def __init__(self, start_response):
        self.start_response = start_response

    def write(self, segment_name, query):
        logging.info('Servicing request: {query}'.format(query=query))
        # if one or more of the query(s) are not a write query, raise an exception.
        if not query:
            raise Exception("No query provided.")
        for q in sqlparse.parse(query):
            if q.get_type() not in ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER']:
                raise Exception('This server only accepts "Write" queries, that is, queries that begin with "INSERT", "UPDATE", "DELETE", "CREATE" or "ALTER".')
        connection = sqlite3.connect(segment_name)
        cursor = connection.cursor()
        try:
            output = cursor.execute(query.decode('utf-8'))
        finally:
            cursor.close()
            connection.commit()
            connection.close()
        return b"OK"

def application(env, start_response):
    try:
        segment_name = env.get('HTTP_HOST', "").split(".")[0] # get database id from host/request path
        query = env.get('wsgi.input').read()
        self.segment_name = segment_name
        self.segment_path = os.path.join(settings['LOCAL_DATA'], "{name}.sqlite".format(name=segment_name))
        return WriteServer(start_response).write(segment_name, query)
    except Exception as e:
        start_response('500 Server Error', [('Content-Type', 'text/plain')])
        return [b'500 Server Error: %s' % str(e).encode('utf-8')]