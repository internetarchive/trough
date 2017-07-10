import rethinkdb as r
import ujson as json
import datetime
import re
import doublethink
import pycurl
from io import BytesIO
import logging
from urllib.parse import urlparse, urlencode
from http.client import HTTPConnection
import socks

def healthy_services_query(rethinker, role):
    return rethinker.table('services').filter({"role": role}).filter(
        lambda svc: r.now().sub(svc["last_heartbeat"]) < svc["ttl"]
    )

class TroughCursor():
    def __init__(self, database=None, rethinkdb=None, proxy=None, proxy_port=9000, proxy_type='SOCKS5'):
        self.database = database
        self.rethinkdb = rethinkdb
        self.proxy = proxy
        self.proxy_port = proxy_port
        self.proxy_type = socks.PROXY_TYPE_SOCKS5 if proxy_type == 'SOCKS5' else socks.PROXY_TYPE_SOCKS4
        # use this flag to save time. don't provision database for each query.
        self._writable = False
        #self.rethinker = doublethink.rethinker()
        self._write_url = None

    def _do_read(self, query, raw=False):
        # send query to server, return JSON
        rethinker = doublethink.Rethinker(db="trough_configuration", servers=self.rethinkdb)
        healthy_databases = list(rethinker.table('services').get_all(self.database, index='segment').run())
        healthy_databases = [db for db in healthy_databases if db['role'] == 'trough-read' and (rethinker.now().run() - db['last_heartbeat']).seconds < db['ttl']]
        try:
            assert len(healthy_databases) > 0
        except:
            raise Exception('No healthy node found for segment %s' % self.database)
        url = urlparse(healthy_databases[0].get('url'))
        if self.proxy:
            conn = HTTPConnection(self.proxy, self.proxy_port)
            conn.set_tunnel(url.netloc, url.port)
            conn.sock = socks.socksocket()
            conn.sock.set_proxy(self.proxy_type, self.proxy, self.proxy_port)
            conn.sock.connect((url.netloc.split(":")[0], url.port))
        else:
            conn = HTTPConnection(url.netloc)
        request_path = "%s?%s" % (url.path, url.query)
        conn.request("POST", request_path, query)
        response = conn.getresponse()
        results = json.loads(response.read())
        self._last_results = results

    def _do_write(self, query):
        # send provision query to server if not self._write_url.
        # after send provision query, set self._write_url.
        # send query to server, return JSON
        rethinker = doublethink.Rethinker(db="trough_configuration", servers=self.rethinkdb)
        services = doublethink.ServiceRegistry(rethinker)
        master_node = services.unique_service('trough-sync-master')
        logging.info('master_node=%r', master_node)
        if not master_node:
            raise Exception('no healthy trough-sync-master in service registry')
        if not self._write_url:
            buffer = BytesIO()
            c = pycurl.Curl()
            c.setopt(c.URL, master_node.get('url'))
            c.setopt(c.POSTFIELDS, self.database)
            if self.proxy:
                c.setopt(pycurl.PROXY, self.proxy)
                c.setopt(pycurl.PROXYPORT, int(self.proxy_port))
                c.setopt(pycurl.PROXYTYPE, self.proxy_type)
            c.setopt(c.WRITEDATA, buffer)
            c.perform()
            c.close()
            self._write_url = buffer.getvalue()
            logging.info('self._write_url=%r', self._write_url)
        buffer = BytesIO()
        c = pycurl.Curl()
        c.setopt(c.URL, self._write_url)
        c.setopt(c.POSTFIELDS, query)
        if self.proxy:
            c.setopt(pycurl.PROXY, self.proxy)
            c.setopt(pycurl.PROXYPORT, int(self.proxy_port))
            c.setopt(pycurl.PROXYTYPE, self.proxy_type)
        c.setopt(c.WRITEDATA, buffer)
        c.perform()
        c.close()
        response = buffer.getvalue()
        if response.strip() != b'OK':
            raise Exception('Trough Query Failed: Database: %r Response: %r Query: %.200r' % (self.database, response, query))
        self._last_results = None
    def execute(self, sql, params=[], force=None, raw=False):
        query = sql % tuple(repr(param) for param in params)
        if force=='read' or query.strip()[:6].lower() == 'select':
            return self._do_read(query, raw)
        return self._do_write(query)
    def executemany(self, queries):
        query_types = set()
        split_queries = sqlparse.split(queries, encoding=None)
        for query in split_queries:
            query_types = (query.strip()[:6].lower() == 'select')
        if len(query_types > 1):
            raise Exception('Queries passed to executemany() must be exclusively SELECT or non-SELECT queries.')
        return self.execute(queries, force='read' if True in query_types else 'write')
    def executescript(self, queries):
        self.executemany(queries)
    def close(self):
        pass
    def fetchall(self):
        return self._last_results
    def fetchmany(self, size=100):
        return self._last_results[0:size]
    def fetchone(self):
        return [v for k,v in self._last_results[0].items()]

class TroughConnection():
    def __init__(self, *args, database=None, rethinkdb=None, proxy=None, proxy_port=9000, proxy_type='SOCKS5', **kwargs):
        self.database = database
        self.rethinkdb = rethinkdb
        self.proxy = proxy
        self.proxy_port = int(proxy_port)
        self.proxy_type = proxy_type
    def cursor(self):
        return TroughCursor(database=self.database,
            rethinkdb=self.rethinkdb,
            proxy=self.proxy,
            proxy_port=self.proxy_port,
            proxy_type=self.proxy_type)
    def execute(self, query):
        return self.cursor().execute(query)
    def executemany(self, queries):
        return self.cursor().executemany(query)
    def executescript(self, queries):
        return self.cursor().executescript(query)
    def close(self):
        pass
    def commit(self):
        pass

def connect(*args, **kwargs):
    return TroughConnection(**kwargs)