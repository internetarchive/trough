#!/usr/bin/env python3
import trough
from trough.settings import settings
import sqlite3
import ujson
import os
import sqlparse
import logging
import requests
from contextlib import closing
import urllib
import doublethink

class ReadServer:
    def proxy_for_write_host(self, node, segment, query):
        # enforce that we are querying the correct database, send an explicit hostname.
        write_url = "http://{node}:{port}/?segment={segment}".format(node=node, segment=segment.id, port=settings['READ_PORT'])
        # this "closing" syntax is recommended here: http://docs.python-requests.org/en/master/user/advanced/
        with closing(requests.post(write_url, stream=True, data=query)) as r:
            status_line = '{status_code} {reason}'.format(status_code=r.status_code, reason=r.reason)
            # headers [('Content-Type','application/json')]
            headers = [("Content-Type", r.headers['Content-Type'],)]
            self.start_response(status_line, headers)
            for chunk in r.iter_content():
                yield chunk

    def read(self, segment, query):
        logging.info('Servicing request: {query}'.format(query=query))
        # if the user sent more than one query, or the query is not a SELECT, raise an exception.
        if len(sqlparse.split(query)) != 1 or sqlparse.parse(query)[0].get_type() != 'SELECT':
            raise Exception('Exactly one SELECT query per request, please.')
        assert os.path.isfile(segment.local_path())
        logging.info("Connecting to sqlite database: {segment}".format(segment=segment.local_path()))
        connection = sqlite3.connect(segment.local_path())
        trough.sync.setup_connection(connection)
        cursor = connection.cursor()
        first = True
        cursor.execute(query.decode('utf-8'))
        self.start_response('200 OK', [('Content-Type','application/json')])
        yield b"["
        try:
            for row in cursor.fetchall():
                if not first:
                    yield b",\n"
                output = dict((cursor.description[i][0], value) for i, value in enumerate(row))
                yield ujson.dumps(output, escape_forward_slashes=False).encode('utf-8')
                first = False
            yield b"]\n"
        finally:
            # close the cursor 'finally', in case there is an Exception.
            cursor.close()
            cursor.connection.close()

    # uwsgi endpoint
    def __call__(self, env, start_response):
        self.start_response = start_response
        try:
            query_dict = urllib.parse.parse_qs(env['QUERY_STRING'])
            # use the ?segment= query string variable or the host string to figure out which sqlite database to talk to.
            segment_id = query_dict.get('segment', env.get('HTTP_HOST', "").split("."))[0]
            logging.info('Connecting to Rethinkdb on: %s' % settings['RETHINKDB_HOSTS'])
            rethinker = doublethink.Rethinker(db="trough_configuration", servers=settings['RETHINKDB_HOSTS'])
            services = doublethink.ServiceRegistry(rethinker)
            registry = trough.sync.HostRegistry(rethinker=rethinker, services=services)
            segment = trough.sync.Segment(segment_id=segment_id, size=0, rethinker=rethinker, services=services, registry=registry)
            trough.sync.ensure_tables(rethinker)
            query = env.get('wsgi.input').read()
            write_lock = segment.retrieve_write_lock()
            if write_lock and write_lock['node'] != settings['HOSTNAME']:
                logging.info('Found write lock for {segment}. Proxying {query} to {host}'.format(segment=segment.id, query=query, host=write_lock['node']))
                return self.proxy_for_write_host(write_lock['node'], segment, query)
            return self.read(segment, query)
        except Exception as e:
            logging.error('500 Server Error due to exception', exc_info=True)
            start_response('500 Server Error', [('Content-Type', 'text/plain')])
            return [('500 Server Error: %s\n' % str(e)).encode('utf-8')]
