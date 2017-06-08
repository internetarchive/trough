#!/usr/bin/env python3
import trough
from trough.settings import settings
import sqlite3
import ujson
import os
import sqlparse
import logging
import urllib
import doublethink

class WriteServer:
    def write(self, segment, query):
        logging.info('Servicing request: segment=%r query=%r', segment, query)
        # if one or more of the query(s) are not a write query, raise an exception.
        if not query:
            raise Exception("No query provided.")
        # no sql parsing, if our chmod has write permission, allow all queries.
        connection = sqlite3.connect(segment.local_path())
        trough.sync.setup_connection(connection)
        try:
            query = query.rstrip();
            if not query[-1] == b';':
                query = query + b';'
            # executescript does not seem to respect isolation_level, so for
            # performance, we wrap the sql in a transaction manually
            # see http://bugs.python.org/issue30593
            query = b"BEGIN TRANSACTION;\n" + query + b"\nCOMMIT;\n"
            output = connection.executescript(query.decode('utf-8'))
        finally:
            connection.commit()
            connection.close()
        return b"OK\n"

    # uwsgi endpoint
    def __call__(self, env, start_response):
        try:
            query_dict = urllib.parse.parse_qs(env.get('QUERY_STRING'))
            # use the ?segment= query string variable or the host string to figure out which sqlite database to talk to.
            segment_id = query_dict.get('segment', env.get('HTTP_HOST', "").split("."))[0]
            logging.info('Connecting to Rethinkdb on: %s' % settings['RETHINKDB_HOSTS'])
            rethinker = doublethink.Rethinker(db="trough_configuration", servers=settings['RETHINKDB_HOSTS'])
            services = doublethink.ServiceRegistry(rethinker)
            registry = trough.sync.HostRegistry(rethinker=rethinker, services=services)
            segment = trough.sync.Segment(segment_id=segment_id, size=0, rethinker=rethinker, services=services, registry=registry)
            query = env.get('wsgi.input').read()
            trough.sync.ensure_tables(rethinker)
            write_lock = segment.retrieve_write_lock()
            if not write_lock or write_lock['node'] != settings['HOSTNAME']:
                raise Exception("This node (settings['HOSTNAME']={!r}) cannot write to segment {!r}. There is no write lock set, or the write lock authorizes another node.".format(settings['HOSTNAME'], segment.id))

            output = self.write(segment, query)
            start_response('200 OK', [('Content-Type', 'text/plain')])
            return output
        except Exception as e:
            logging.error('segment=%r query=%r', segment, query, exc_info=True)
            start_response('500 Server Error', [('Content-Type', 'text/plain')])
            return [('500 Server Error: %s\n' % str(e)).encode('utf-8')]
