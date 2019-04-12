'''
trough/client.py - trough client code

Copyright (C) 2017-2019 Internet Archive

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301,
USA.
'''

from __future__ import absolute_import

import logging
import os
import json
import requests
import doublethink
import rethinkdb as r
import datetime
import threading
import time
import collections
from aiohttp import ClientSession

class TroughException(Exception):
    pass

class TroughNoReadUrlException(TroughException):
    pass

class TroughClient(object):
    logger = logging.getLogger('trough.client.TroughClient')

    def __init__(self, rethinkdb_trough_db_url, promotion_interval=None):
        '''
        TroughClient constructor

        Args:
            rethinkdb_trough_db_url: url with schema rethinkdb:// pointing to
                trough configuration database
            promotion_interval: if specified, `TroughClient` will spawn a
                thread that "promotes" (pushed to hdfs) "dirty" trough segments
                (segments that have received writes) periodically, sleeping for
                `promotion_interval` seconds between cycles (default None)
        '''
        parsed = doublethink.parse_rethinkdb_url(rethinkdb_trough_db_url)
        self.rr = doublethink.Rethinker(
                servers=parsed.hosts, db=parsed.database)
        self.svcreg = doublethink.ServiceRegistry(self.rr)
        self._write_url_cache = {}
        self._read_url_cache = {}
        self._dirty_segments = set()
        self._dirty_segments_lock = threading.RLock()

        self.promotion_interval = promotion_interval
        self._promoter_thread = None
        if promotion_interval:
            self._promoter_thread = threading.Thread(
                    target=self._promotrix, name='TroughClient-promoter')
            self._promoter_thread.setDaemon(True)
            self._promoter_thread.start()

    def _promotrix(self):
        while True:
            time.sleep(self.promotion_interval)
            try:
                with self._dirty_segments_lock:
                    dirty_segments = list(self._dirty_segments)
                    self._dirty_segments.clear()
                self.logger.info(
                        'promoting %s trough segments', len(dirty_segments))
                for segment_id in dirty_segments:
                    try:
                        self.promote(segment_id)
                    except:
                        self.logger.error(
                                'problem promoting segment %s', segment_id,
                                exc_info=True)
            except:
                self.logger.error(
                        'caught exception doing segment promotion',
                        exc_info=True)

    def promote(self, segment_id):
        url = os.path.join(self.segment_manager_url(), 'promote')
        payload_dict = {'segment': segment_id}
        self.logger.debug('posting %s to %s', json.dumps(payload_dict), url)
        response = requests.post(url, json=payload_dict, timeout=21600)
        if response.status_code != 200:
            raise TroughException(
                    'unexpected response %r %r: %r from POST %r with '
                    'payload %r' % (
                        response.status_code, response.reason, response.text,
                        url, json.dumps(payload_dict)))

    @staticmethod
    def sql_value(x):
        if x is None:
            return 'null'
        elif isinstance(x, datetime.datetime):
            return 'datetime(%r)' % x.isoformat()
        elif isinstance(x, bool):
            return int(x)
        elif isinstance(x, str) or isinstance(x, bytes):
            # the only character that needs escaped in sqlite string literals
            # is single-quote, which is escaped as two single-quotes
            if isinstance(x, bytes):
                s = x.decode('utf-8')
            else:
                s = x
            return "'" + s.replace("'", "''") + "'"
        elif isinstance(x, (int, float)):
            return x
        else:
            raise TroughException(
                    "don't know how to make an sql value from %r (%r)" % (
                        x, type(x)))

    def segment_manager_url(self):
        master_node = self.svcreg.unique_service('trough-sync-master')
        if not master_node:
            raise TroughException(
                    'no healthy trough-sync-master in service registry')
        return master_node['url']

    def write_url_nocache(self, segment_id, schema_id='default'):
        url = os.path.join(self.segment_manager_url(), 'provision')
        payload_dict = {'segment': segment_id, 'schema': schema_id}
        self.logger.debug('posting %s to %s', json.dumps(payload_dict), url)
        response = requests.post(url, json=payload_dict, timeout=600)
        if response.status_code != 200:
            raise TroughException(
                    'unexpected response %r %r: %r from POST %r with '
                    'payload %r' % (
                        response.status_code, response.reason, response.text,
                        url, json.dumps(payload_dict)))
        result_dict = response.json()
        # assert result_dict['schema'] == schema_id  # previously provisioned?
        return result_dict['write_url']

    def read_url_nocache(self, segment_id):
        reql = self.rr.table('services').get_all(
                segment_id, index='segment').filter(
                        {'role':'trough-read'}).filter(
                                lambda svc: r.now().sub(
                                    svc['last_heartbeat']).lt(svc['ttl'])
                                ).order_by('load')
        self.logger.debug('querying rethinkdb: %r', reql)
        results = reql.run()
        try:
            return results[0]['url']
        except:
            raise TroughNoReadUrlException(
                    'no read url for segment %s; usually this means the '
                    "segment hasn't been provisioned yet" % segment_id)

    def read_urls_for_regex(self, regex):
        '''
        Looks up read urls for segments matching `regex`.
        Populates `self._read_url_cache` and returns dictionary
        `{segment: url}`
        '''
        d = {}
        reql = self.rr.table('services')\
                .filter({'role': 'trough-read'})\
                .filter(r.row.has_fields('segment'))\
                .filter(lambda svc: svc['segment'].coerce_to('string').match(regex))\
                .filter(lambda svc: r.now().sub(svc['last_heartbeat']).lt(svc['ttl']))
        self.logger.debug('querying rethinkdb: %r', reql)
        results = reql.run()
        for result in results:
            d[result['segment']] = result['url']
            self._read_url_cache[result['segment']] = result['url']
        return d

    def schemas(self):
        reql = self.rr.table('schema')
        for result in reql.run():
            yield collections.OrderedDict([('name', result['id'])])

    def schema(self, id):
        reql = self.rr.table('schema').get(id)
        result = reql.run()
        if result:
            return [collections.OrderedDict([(id, result['sql'])])]
        else:
            return None

    def readable_segments(self, regex=None):
        reql = self.rr.table('services').filter(
                {'role':'trough-read'}).filter(
                        lambda svc: r.now().sub(
                            svc['last_heartbeat']).lt(svc['ttl'])
                        )# .order_by('segment')
        if regex:
            reql = reql.filter(lambda svc: svc['segment'].match(regex))
        self.logger.debug('querying rethinkdb: %r', reql)
        results = reql.run()
        for result in reql.run():
            yield collections.OrderedDict([
                    ('segment', result['segment']),
                    ('url', result['url']),
                    ('first_heartbeat', result['first_heartbeat']),
                    ('last_heartbeat', result['last_heartbeat'])])

    def write_url(self, segment_id, schema_id='default'):
        if not segment_id in self._write_url_cache:
            self._write_url_cache[segment_id] = self.write_url_nocache(
                    segment_id, schema_id)
            self.logger.info(
                    'segment %r write url is %r', segment_id,
                    self._write_url_cache[segment_id])
        return self._write_url_cache[segment_id]

    def read_url(self, segment_id):
        if not self._read_url_cache.get(segment_id):
            self._read_url_cache[segment_id] = self.read_url_nocache(segment_id)
            self.logger.info(
                    'segment %r read url is %r', segment_id,
                    self._read_url_cache[segment_id])
        return self._read_url_cache[segment_id]

    def write(self, segment_id, sql_tmpl, values=(), schema_id='default'):
        write_url = self.write_url(segment_id, schema_id)
        sql = sql_tmpl % tuple(self.sql_value(v) for v in values)
        sql_bytes = sql.encode('utf-8')

        try:
            response = requests.post(
                    write_url, sql_bytes, timeout=600,
                    headers={'content-type': 'application/sql;charset=utf-8'})
            if response.status_code != 200:
                raise TroughException(
                        'unexpected response %r %r: %r from POST %r with '
                        'payload %r' % (
                            response.status_code, response.reason,
                            response.text, write_url, sql_bytes))
            if segment_id not in self._dirty_segments:
                with self._dirty_segments_lock:
                    self._dirty_segments.add(segment_id)
        except Exception as e:
            self._write_url_cache.pop(segment_id, None)
            raise e

    def read(self, segment_id, sql_tmpl, values=()):
        read_url = self.read_url(segment_id)
        sql = sql_tmpl % tuple(self.sql_value(v) for v in values)
        sql_bytes = sql.encode('utf-8')
        try:
            response = requests.post(
                    read_url, sql_bytes, timeout=600,
                    headers={'content-type': 'application/sql;charset=utf-8'})
            if response.status_code != 200:
                raise TroughException(
                        'unexpected response %r %r %r from %r to query %r' % (
                            response.status_code, response.reason, response.text,
                            read_url, sql_bytes))
            self.logger.trace(
                    'got %r from posting query %r to %r', response.text, sql,
                    read_url)
            results = json.loads(response.text)
            return results
        except Exception as e:
            self._read_url_cache.pop(segment_id, None)
            raise e

    async def async_read(self, segment_id, sql_tmpl, values=()):
        read_url = self.read_url(segment_id)
        sql = sql_tmpl % tuple(self.sql_value(v) for v in values)
        sql_bytes = sql.encode('utf-8')

        async with ClientSession() as session:
            async with session.post(
                    read_url, data=sql_bytes, headers={
                        'content-type': 'application/sql;charset=utf-8'}) as res:
                if res.status != 200:
                    self._read_url_cache.pop(segment_id, None)
                    text = await res.text('utf-8')
                    raise TroughException(
                            'unexpected response %r %r %r from %r to '
                            'query %r' % (
                                res.status, res.reason, text, read_url,
                                sql))
                results = list(await res.json())
                return results

    def schema_exists(self, schema_id):
        url = os.path.join(self.segment_manager_url(), 'schema', schema_id)
        response = requests.get(url, timeout=60)
        if response.status_code == 200:
            return True
        elif response.status_code == 404:
            return False
        else:
            try:
                response.raise_for_status()
            except Exception as e:
                raise TroughException(e)

    def register_schema(self, schema_id, sql):
        url = os.path.join(
                self.segment_manager_url(), 'schema', schema_id, 'sql')
        response = requests.put(url, sql, timeout=600)
        if response.status_code not in (201, 204):
            raise TroughException(
                    'unexpected response %r %r %r from %r to query %r' % (
                        response.status_code, response.reason, response.text,
                        url, sql))

