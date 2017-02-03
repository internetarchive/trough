#!/usr/bin/env python3
import trough
from trough.settings import settings
import sqlite3
import ujson
import os
import sqlparse
import logging
import consulate
import requests
from contextlib import closing

class ReadServer:
    def proxy_for_write_host(self, segment, query):
        # enforce that we are querying the correct database, send an explicit hostname.
        write_url = "http://{segment}.trough-write-segments.service.consul:{port}/".format(segment=segment.id, port=settings['READ_PORT'])
        # this "closing" syntax is recommended here: http://docs.python-requests.org/en/master/user/advanced/
        with closing(requests.post(write_url, stream=True, data=query)) as r:
            # todo: get the response code and the content type header from the response.
            status_line = '{status_code} {reason}'.format(status_code=r.status_code, reason=r.reason)
            # headers [('Content-Type','application/json')]
            headers = [("Content-Type", r.headers['Content-Type'],)]
            self.start_response(status_line, headers)
            return r.iter_content()

    def read(self, segment, query):
        logging.info('Servicing request: {query}'.format(query=query))
        # if the user sent more than one query, or the query is not a SELECT, raise an exception.
        if len(sqlparse.split(query)) != 1 or sqlparse.parse(query)[0].get_type() != 'SELECT':
            raise Exception('Exactly one SELECT query per request, please.')
        connection = sqlite3.connect(segment.segment_path())
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
    def __call__(self, env, start_response):
        self.start_response = start_response
        try:
            segment_id = env.get('HTTP_HOST', "").split(".")[0] # get database id from host/request path
            consul = consulate.Consul(host=settings['CONSUL_ADDRESS'], port=settings['CONSUL_PORT'])
            registry = HostRegistry(consul=consul)
            segment = trough.sync.Segment(segment_id=segment_id)
            query = env.get('wsgi.input').read()
            write_lock = segment.retrieve_write_lock()
            if write_lock and write_lock['Node'] != settings['HOSTNAME']:
                logging.info('Found write lock for {segment}. Proxying {query} to {host}'.format(segment=segment.id, query=query, host=write_lock['Node']))
                return self.proxy_for_write_host(segment, query)
            return self.read(segment, query)
        except Exception as e:
            start_response('500 Server Error', [('Content-Type', 'text/plain')])
            return [b'500 Server Error: %s' % str(e).encode('utf-8')]

# setup uwsgi endpoint
application = ReadServer()
