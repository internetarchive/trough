#!/usr/bin/env python3
# TODO: we also need to solve streaming. Maybe ijson?
#from application import app
from bin.settings import settings
import sqlite3
import ujson
import os
import sqlparse
import logging
#from mixins import ThreadPoolMixIn
#from http.server import HTTPServer, BaseHTTPRequestHandler

# class ReadHandler(BaseHTTPRequestHandler):
#     def do_POST(self):
#         database = os.path.split(self.path)[-1]
#         database = os.path.join(settings['DATA_ROOT'], "{name}.sqlite".format(name=database))
#         connection = sqlite3.connect(database)
#         cur = connection.cursor()

#         query = self.rfile.readline().strip()
#         logging.info('Servicing request: {query}'.format(query=query))
#         if len(sqlparse.split(query)) != 1:
#             raise Exception('Exactly one SELECT query at a time.')
#         parsed = sqlparse.parse(query)

#         cur.execute(query.decode('utf-8'))
#         output = [dict((cur.description[i][0], value) \
#             for i, value in enumerate(row)) for row in cur.fetchall()]
#         cur.close()
#         connection.close()

#         return self.wfile.write(ujson.dumps(output).encode('utf-8'))

#class ThreadedHTTPServer(ThreadPoolMixIn, HTTPServer):
#    """Handle requests in a separate thread."""
#    numThreads = int(settings.get('READ_THREADS', 8))

#server = ThreadedHTTPServer(('0.0.0.0', 8080), ReadHandler)
#logging.warn('Starting server with {threads} threads, use <Ctrl-C> to stop'.format(threads=server.numThreads))
#server.serve_forever()

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

        cur.execute(query.decode('utf-8'))
        output = [dict((cur.description[i][0], value) \
            for i, value in enumerate(row)) for row in cur.fetchall()]

        cur.close()
        connection.close()
        start_response('200 OK', [('Content-Type','application/json')])
        return [ujson.dumps(output).encode('utf-8')]

    except Exception as e:
        start_response('500 Server Error', [('Content-Type', 'text/plain')])
        return [b'500 Server Error: %s' % str(e).encode('utf-8')]