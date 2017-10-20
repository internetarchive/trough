#!/usr/bin/env python3
import logging
import doublethink
import rethinkdb as r
from trough.settings import settings
from snakebite import client
import socket
import json
import os
import time
import random
import sys
import string
import requests
import datetime
import sqlite3
import re
import contextlib
from uhashring import HashRing
import ujson

def healthy_services_query(rethinker, role):
    return rethinker.table('services').filter({"role": role}).filter(
        lambda svc: r.now().sub(svc["last_heartbeat"]).lt(svc["ttl"])
    )

def setup_connection(conn):
    def regexp(expr, item):
        try:
            if item is None:
                return False
            reg = re.compile(expr)
            return reg.search(item) is not None
        except:
            logging.error('REGEXP(%r, %r)', expr, item, exc_info=True)
            raise

    def seed_crawled_status_filter(status_code):
        ''' convert crawler status codes to human-readable test '''
        try:
            status_code = int(status_code)
        except TypeError:
            return 'Not crawled (%s)' % status_code

        if status_code >= 300 and status_code < 400:
            return 'Redirected'
        elif status_code >= 400:
            return 'Crawled (HTTP error %s)' % status_code
        elif status_code > 0:
            return 'Crawled'
        elif status_code == 0:
            return 'Not crawled (queued)'
        elif status_code == -9998:
            return 'Not crawled (blocked by robots)'
        else:
            return 'Not crawled (%s)' % status_code

    def build_redirect_array(redirect_url, redirect_status, hop_path, json_list=None):
        hop_no = len(hop_path) # parse out "[0-9]+"? how many times will we have 50+ redirects for seeds? maybe 0.
        if json_list:
            json_list = json.loads(json_list)
        else:
            json_list = []
        if hop_no > len(json_list):
            json_list.extend(None for i in range(hop_no - len(json_list)))
        redirect = {'seed': redirect_url, 'status': seed_crawled_status_filter(redirect_status) }
        json_list[(hop_no-1)] = redirect
        return json.dumps(json_list)

    conn.create_function('REGEXP', 2, regexp)
    conn.create_function('SEEDCRAWLEDSTATUS', 1, seed_crawled_status_filter)
    conn.create_function('BUILDREDIRECTARRAY', 4, build_redirect_array)

class AssignmentQueue:
    def __init__(self, rethinker):
        self._queue = []
        self.rethinker = rethinker
    def enqueue(self, item):
        self._queue.append(item)
        if self.length() >= 1000:
            self.commit()
    def commit(self):
        self.rethinker.table('assignment').insert(self._queue).run();
        del self._queue[:]
    def length(self):
        return len(self._queue)

class Assignment(doublethink.Document):
    def populate_defaults(self):
        if not "id" in self:
            self.id = "{node}:{segment}".format(node=self.node, segment=self.segment)
            self._pk = "id"
    @classmethod
    def table_create(cls, rr):
        rr.table_create(cls.table).run()
        rr.table(cls.table).index_create('segment').run()
        rr.table(cls.table).index_wait('segment').run()
    @classmethod
    def host_assignments(cls, rr, node):
        return (Assignment(rr, d=asmt) for asmt in rr.table(cls.table).between('%s:\x01' % node, '%s:\x7f' % node, right_bound="closed").run())
    @classmethod
    def all(cls, rr):
        return (Assignment(rr, d=asmt) for asmt in rr.table(cls.table).run())
    @classmethod
    def segment_assignments(cls, rr, segment):
        return (Assignment(rr, d=asmt) for asmt in rr.table(cls.table).get_all(segment, index="segment").run())
    def unassign(self):
        return self.rr.table(self.table).get(self.id).delete().run()

class Lock(doublethink.Document):
    @classmethod
    def table_create(cls, rr):
        rr.table_create(cls.table).run()
        rr.table(cls.table).index_create('node').run()
        rr.table(cls.table).index_wait('node').run()
    @classmethod
    def acquire(cls, rr, pk, document={}):
        '''Acquire a lock. Raises an exception if the lock key exists.'''
        document["id"] = pk
        document["node"] = settings['HOSTNAME']
        document["acquired_on"] = r.now()
        output = rr.table(cls.table).insert(document).run()
        if output.get('errors'):
            raise Exception('Unable to acquire a lock for id: "%s"' % pk)
        return cls(rr, d=document)
    def release(self):
        return self.rr.table(self.table, read_mode='majority').get(self.id).delete().run()
    @classmethod
    def host_locks(cls, rr, host):
        return (Lock(rr, d=asmt) for asmt in rr.table(cls.table).get_all(host, index="node").run())

class Schema(doublethink.Document):
    pass

def init(rethinker):
    Assignment.table_ensure(rethinker)
    Lock.table_ensure(rethinker)
    Schema.table_ensure(rethinker)
    default_schema = Schema.load(rethinker, 'default')
    if not default_schema:
        default_schema = Schema(rethinker, d={'sql':''})
        default_schema.id = 'default'
        logging.info('saving default schema %r', default_schema)
        default_schema.save()
    else:
        logging.info('default schema already exists %r', default_schema)
    try:
        rethinker.table('services').index_create('segment').run()
        rethinker.table('services').index_wait('segment').run()
        rethinker.table('services').index_create('role').run()
        rethinker.table('services').index_wait('role').run()
    except Exception as e:
        pass

    snakebite_client = client.Client(settings['HDFS_HOST'], settings['HDFS_PORT'])
    for d in snakebite_client.mkdir([settings['HDFS_PATH']], create_parent=True):
        logging.info('created hdfs dir %r', d)

class Segment(object):
    def __init__(self, segment_id, size, rethinker, services, registry, remote_path=None):
        self.id = segment_id
        self.size = int(size)
        self.rethinker = rethinker
        self.services = services
        self.registry = registry
        self._remote_path = remote_path
    def host_key(self, host):
        return "%s:%s" % (host, self.id)
    def all_copies(self):
        ''' returns the 'assigned' segment copies, whether or not they are 'up' '''
        return Assignment.segment_assignments(self.rethinker, self.id)
    def readable_copies_query(self):
        return self.rethinker.table('services').get_all(self.id, index="segment").filter({"role": 'trough-read'}).filter(
                lambda svc: r.now().sub(svc["last_heartbeat"]).lt(svc["ttl"])
            )
    def readable_copies(self):
        '''returns the 'up' copies of this segment to read from, per rethinkdb.'''
        return self.readable_copies_query().run()
    def readable_copies_count(self):
        '''returns the count of 'up' copies of this segment to read from, per rethinkdb.'''
        return self.readable_copies_query().count().run()
    def writable_copies_query(self):
        return healthy_services_query(self.rethinker, role='trough-write').filter({ 'segment': self.id })
    def writable_copy(self):
        '''returns the 'up' copies of this segment to write to, per rethinkdb.'''
        copies = list(self.writable_copies_query().run())
        if copies:
            return copies[0]
        return None
    def is_assigned_to_host(self, host):
        return bool(Assignment.load(self.rethinker, self.host_key(host)))
    def minimum_assignments(self):
        '''This function should return the minimum number of assignments which is acceptable for a given segment.'''
        if hasattr(settings['MINIMUM_ASSIGNMENTS'], "__call__"):
            return settings['MINIMUM_ASSIGNMENTS'](self.id)
        else:
            return settings['MINIMUM_ASSIGNMENTS']
    def acquire_write_lock(self):
        '''Raises exception if lock exists.'''
        return Lock.acquire(self.rethinker, pk='write:lock:%s' % self.id, document={ "segment": self.id })
    def retrieve_write_lock(self):
        '''Returns None or dict. Can be used to evaluate whether a lock exists and, if so, which host holds it.'''
        return Lock.load(self.rethinker, 'write:lock:%s' % self.id)
    def local_host_can_write(self):
        write_lock = self.retrieve_write_lock()
        if write_lock and write_lock['node'] == settings['HOSTNAME']:
            return write_lock
        else:
            return None
    def local_path(self):
        return os.path.join(settings['LOCAL_DATA'], "%s.sqlite" % self.id)
    def remote_path(self):
        if self._remote_path:
            return self._remote_path
        asmt = Assignment.load(self.rethinker, self.host_key(settings['HOSTNAME']))
        if asmt:
            return asmt.remote_path
        return ""
    def local_segment_exists(self):
        return os.path.isfile(self.local_path())
    def provision_local_segment(self):
        connection = sqlite3.connect(self.local_path())
        setup_connection(connection)
        cursor = connection.cursor()
        if settings['SEGMENT_INITIALIZATION_SQL']:
            with open(settings['SEGMENT_INITIALIZATION_SQL'], 'r') as script:
                query = script.read()
                cursor.executescript(query)
        cursor.close()
        connection.commit()
        connection.close()
    def __repr__(self):
        return '<Segment:id=%r,local_path=%r>' % (self.id, self.local_path())

class HostRegistry(object):
    ''''''
    def __init__(self, rethinker, services):
        self.rethinker = rethinker
        self.services = services
        self.assignment_queue = AssignmentQueue(self.rethinker)
    def get_hosts(self):
        return list(self.rethinker.table('services').between('trough-nodes:!', 'trough-nodes:~').filter(
                   lambda svc: r.now().sub(svc["last_heartbeat"]).lt(svc["ttl"])
               ).order_by("load").run())
    def total_bytes_for_node(self, node):
        for service in self.services.available_services('trough-nodes'):
            if service['node'] == node:
                return service.get('available_bytes')
        raise Exception('Could not find node "%s"' % node)
    def heartbeat(self, pool=None, node=None, ttl=None, **doc):
        if None in [pool, node, ttl]:
            raise Exception('"pool", "node" and "ttl" are required arguments.')
        doc['id'] = "%s:%s:%s" % (pool, node, doc.get('segment'))
        logging.info("Setting Heartbeat ID to [%s]" % doc['id'])
        doc['role'] = pool
        doc['node'] = node
        doc['ttl'] = ttl
        doc['load'] = os.getloadavg()[1] # load average over last 5 mins
        logging.info('Heartbeat: role[%s] node[%s] at IP %s:%s with ttl %s' % (pool, node, node, doc.get('port'), ttl))
        return self.services.heartbeat(doc)
    def bulk_heartbeat(self, ids):
        self.rethinker.table('services').get_all(*ids).update({ 'last_heartbeat': r.now(), 'load': os.getloadavg()[1] }).run()
        # send a non-bulk heartbeat for each id we *didn't* just update
        missing_ids = set(ids) - set(self.rethinker.table('services').get_all(*ids).get_field('id').run())
        for id in missing_ids:
            pool, node, segment = id.split(":")
            port = settings['WRITE_PORT'] if pool == 'trough-write' else settings['READ_PORT']
            url = 'http://%s:%s/?segment=%s' % (node, port, segment)
            self.heartbeat(pool=pool, node=node, segment=segment, port=port, url=url, ttl=round(settings['SYNC_LOOP_TIMING'] * 4))
    def assign(self, hostname, segment, remote_path):
        logging.info("Assigning segment: %s to '%s'" % (segment.id, hostname))
        asmt = Assignment(self.rethinker, d={ 
            'node': hostname,
            'segment': segment.id,
            'assigned_on': r.now(),
            'remote_path': remote_path,
            'bytes': segment.size })
        logging.info('Adding "%s" to rethinkdb.' % (asmt))
        self.assignment_queue.enqueue(asmt)
        return asmt
    def commit_assignments(self):
        self.assignment_queue.commit()
    def segments_for_host(self, host):
        assignments = Assignment.host_assignments(self.rethinker, host)
        segments = [Segment(segment_id=asmt.segment, size=asmt.bytes, rethinker=self.rethinker, services=self.services, registry=self) for asmt in assignments]
        locks = Lock.host_locks(self.rethinker, host) # TODO: does this need deduplication with the above? We are trading a little overhead for lower complexity...
        segments += [Segment(segment_id=lock.segment, size=0, rethinker=self.rethinker, services=self.services, registry=self) for lock in locks]
        logging.info('Checked for segments assigned to %s: Found %s segment(s)' % (host, len(segments)))
        return segments

# Base class, not intended for use.
class SyncController:
    def __init__(self, rethinker=None, services=None, registry=None, hdfs_path=None):
        self.rethinker = rethinker
        self.services = services
        self.registry = registry
        self.leader = False

        self.hostname = settings['HOSTNAME']
        self.external_ip = settings['EXTERNAL_IP']
        self.rethinkdb_hosts = settings['RETHINKDB_HOSTS']

        self.hdfs_path = settings['HDFS_PATH']
        self.hdfs_host = settings['HDFS_HOST']
        self.hdfs_port = settings['HDFS_PORT']

        self.election_cycle = settings['ELECTION_CYCLE']
        self.sync_server_port = settings['SYNC_SERVER_PORT']
        self.sync_local_port = settings['SYNC_LOCAL_PORT']
        self.read_port = settings['READ_PORT']
        self.write_port = settings['WRITE_PORT']
        self.sync_loop_timing = settings['SYNC_LOOP_TIMING']

        self.rethinkdb_hosts = settings['RETHINKDB_HOSTS']
        self.host_check_wait_period = settings['HOST_CHECK_WAIT_PERIOD']

        self.local_data = settings['LOCAL_DATA']
        self.storage_in_bytes = settings['STORAGE_IN_BYTES']
    def check_config(self):
        raise Exception('Not Implemented')
    def get_segment_file_list(self):
        logging.info('Getting segment list...')
        snakebite_client = client.Client(settings['HDFS_HOST'], settings['HDFS_PORT'])
        return snakebite_client.ls([settings['HDFS_PATH']])
    def get_segment_file_size(self, segment):
        snakebite_client = client.Client(settings['HDFS_HOST'], settings['HDFS_PORT'])
        sizes = [file['length'] for file in snakebite_client.ls([segment.remote_path()])]
        if len(sizes) > 1:
            raise Exception('Received more than one file listing.')
        return sizes[0]

# Master or "Server" mode synchronizer.
class MasterSyncController(SyncController):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.current_master = {}
        self.current_host_nodes = []

    def check_config(self):
        try:
            assert settings['HDFS_PATH'], "HDFS_PATH must be set, otherwise I don't know where to look for sqlite files."
            assert settings['HDFS_HOST'], "HDFS_HOST must be set, or I can't communicate with HDFS."
            assert settings['HDFS_PORT'], "HDFS_PORT must be set, or I can't communicate with HDFS."
            assert settings['ELECTION_CYCLE'] > 0, "ELECTION_CYCLE must be greater than zero. It governs the number of seconds in a sync master election period."
            assert settings['HOSTNAME'], "HOSTNAME must be set, or I can't figure out my own hostname."
            assert settings['EXTERNAL_IP'], "EXTERNAL_IP must be set. We need to know which IP to use."
            assert settings['SYNC_SERVER_PORT'], "SYNC_SERVER_PORT must be set. We need to know the output port."
            assert settings['RETHINKDB_HOSTS'], "RETHINKDB_HOSTS must be set. Where can I contact RethinkDB on port 29015?"
        except AssertionError as e:
            sys.exit("{} Exiting...".format(str(e)))

    def hold_election(self):
        logging.info('Holding Sync Master Election...')
        candidate = {
            "id": "trough-sync-master",
            "node": self.hostname,
            "port": self.sync_server_port,
            "url": "http://%s:%s/" % (self.hostname, self.sync_server_port),
            "ttl": self.election_cycle + self.sync_loop_timing * 4,
        }
        sync_master = self.services.unique_service('trough-sync-master', candidate=candidate)
        if sync_master.get('node') == self.hostname:
            if self.current_master.get('node') != sync_master.get('node'):
                logging.info('I am the new master! url=%r ttl=%r', sync_master.get('url'), sync_master.get('ttl'))
            else:
                logging.debug('I am still the master. url=%r ttl=%r', sync_master.get('url'), sync_master.get('ttl'))
            self.current_master = sync_master
            return True
        else:
            logging.debug('I am not the master. The master is %r', sync_master.get('url'))
            self.current_master = sync_master
            return False

    def assign_segments(self):
        logging.debug('Assigning and balancing segments...')
        max_copies = settings['MAXIMUM_ASSIGNMENTS']
        if self.hold_election():
            last_heartbeat = datetime.datetime.now()
        else:
            return False

        # get segment list
        # output is like ({ "path": "/a/b/c/segmentA.sqlite" }, { "path": "/a/b/c/segmentB.sqlite" })
        segment_files = self.get_segment_file_list()
        # output is like [Segment("segmentA"), Segment("segmentB")]
        segments = []
        for file in segment_files:
            segment = Segment(
                segment_id=file['path'].split('/')[-1].replace('.sqlite', ''),
                size=file['length'],
                remote_path=file['path'],
                rethinker=self.rethinker,
                services=self.services,
                registry=self.registry)
            segments.append(segment) # TODO: fix this per comment above.
        logging.info('assigning and balancing %r segments', len(segments))

        # host_ring_mapping will be e.g. { 'host1': { 'ring': 0, 'weight': 188921 }, 'host2': { 'ring': 0, 'weight': 190190091 }... }
        # the keys are node names, the values are array indices for the hash_rings variable (below)
        host_ring_mapping = Assignment.load(self.rethinker, "ring-assignments")
        if not host_ring_mapping:
            host_ring_mapping = Assignment(self.rethinker, { "id": "ring-assignments" })

        host_weights = {host['node']: self.registry.total_bytes_for_node(host['node'])
                        for host in self.registry.get_hosts()}

        # instantiate N hash rings where N is the lesser of (the maximum number of copies of any segment)
        # and (the number of currently available hosts)
        hash_rings = []
        for i in range(min(max_copies, len(host_weights))):
            ring = HashRing()
            ring.id = i
            hash_rings.append(ring)

        # prune hosts that don't exist anymore
        for host in [key for key in host_ring_mapping.keys() if key not in host_weights and key != 'id']:
            logging.info('pruning worker %r from pool (worker went offline?)', host)
            del(host_ring_mapping[host])

        # assign each host to one hash ring. Save the assignment in rethink so it's reproducible.
        # weight each host assigned to a hash ring with its total assignable bytes quota
        for hostname in [key for key in host_ring_mapping.keys() if key != 'id']:
            host = host_ring_mapping[hostname]
            hash_rings[host['ring']].add_node(hostname, { 'weight': host['weight'] })
            logging.info("Host '%s' assigned to ring %s" % (hostname, host['ring']))

        new_hosts = [host for host in host_weights if host not in host_ring_mapping]
        for host in new_hosts:
            weight = host_weights[host]
            host_ring = sorted(hash_rings, key=lambda ring: len(ring.get_nodes()))[0].id
            host_ring_mapping[host] = { 'weight': weight, 'ring': host_ring }
            hash_rings[host_ring].add_node(host, { 'weight': weight })
            logging.info("new trough worker %r assigned to ring %r", host, host_ring)

        host_ring_mapping.save()

        # 'ring_assignments' will be like { "0-192811": Assignment(), "1-192811": Assignment()... }
        ring_assignments = {}
        for assignment in Assignment.all(self.rethinker):
            if assignment.id != 'ring-assignments':
                dict_key = "%s-%s" % (assignment.hash_ring, assignment.segment)
                ring_assignments[dict_key] = assignment

        # for each segment in segment list:
        for segment in segments:
            # if it's been over 80% of an election cycle since the last heartbeat, hold an election so we don't lose master status
            if datetime.datetime.now() - datetime.timedelta(seconds=0.8 * self.election_cycle) > last_heartbeat:
                if self.hold_election():
                    last_heartbeat = datetime.datetime.now()
                else:
                    return False
            logging.info("Assigning segment [%s]" % (segment.id))
            # find position of segment in N hash rings, where N is the minimum number of assignments for this segment
            random.seed(segment.id) # (seed random so we always get the same sample of hash rings for this item)
            assigned_rings = random.sample(hash_rings, segment.minimum_assignments())
            logging.info("Segment [%s] will use rings %s" % (segment.id, [s.id for s in assigned_rings]))
            for ring in assigned_rings:
                # get the node for the key from hash ring, updating or creating assignments from corresponding entry in 'ring_assignments' as necessary
                assigned_node = ring.get_node(segment.id)
                dict_key = "%s-%s" % (ring.id, segment.id)
                assignment = ring_assignments.get(dict_key)
                logging.info("Current assignment: '%s' New assignment: '%s'" % (assignment.node if assignment else None, assigned_node))
                if assignment is None or assignment.node != assigned_node:
                    logging.info("Segment [%s] will be assigned to host '%s' for ring [%s]" % (segment.id, assigned_node, ring.id))
                    if assignment:
                        logging.info("Removing old assignment to node '%s' for segment [%s]: (%s will be deleted)" % (assignment.node, segment.id, assignment))
                        assignment.unassign()
                        del assignment['id']
                    ring_assignments[dict_key] = ring_assignments.get(dict_key, Assignment(self.rethinker, d={ 
                                                        'hash_ring': ring.id,
                                                        'node': assigned_node,
                                                        'segment': segment.id,
                                                        'assigned_on': r.now(),
                                                        'remote_path': segment.remote_path(),
                                                        'bytes': segment.size }))
                    ring_assignments[dict_key]['node'] = assigned_node
                    ring_assignments[dict_key]['id'] = "%s:%s" % (ring_assignments[dict_key]['node'], ring_assignments[dict_key]['segment'])
                    self.registry.assignment_queue.enqueue(ring_assignments[dict_key])
        logging.info("Committing %s assignments" % (self.registry.assignment_queue.length()))
        # commit assignments that were created or updated
        self.registry.commit_assignments()


    def sync(self):
        '''
        "server" mode:
        - if I am not the leader, poll forever
        - if there are no hosts to assign to, poll forever.
        - for entire list of segments that match pattern in REMOTE_DATA setting:
            - check rethinkdb to make sure each item is assigned to a worker
            - if it is not assigned:
                - assign it using consistent hash rings, based on the available quota on each worker
        '''
        if self.hold_election():
            new_host_nodes = sorted([host.get('node') for host in self.registry.get_hosts()])
            if new_host_nodes != self.current_host_nodes:
                logging.info('pool of trough workers changed size from %r to %r (old=%r new=%r)', len(self.current_host_nodes), len(new_host_nodes), self.current_host_nodes, new_host_nodes)
                self.current_host_nodes = new_host_nodes
            if new_host_nodes:
                self.assign_segments()
            else:
                logging.info('not assigning segments because there are no trough workers!')

    def provision_writable_segment(self, segment_id, schema='default'):
        # the query below implements this algorithm:
        # - look up a write lock for the passed-in segment
        # - if the write lock exists, return it. else:
        #     - look up the set of readable copies of the segment
        #     - if readable copies exist choose the one with the lowest load, and return it. else:
        #         - look up the current set of nodes, choose the one with the lowest load and return it
        assignment = self.rethinker.table('lock')\
            .get('write:lock:%s' % segment_id)\
            .default(r.table('services')\
                .get_all(segment_id, index='segment')\
                .filter({'role':'trough-read'})\
                .filter(lambda svc: r.now().sub(svc["last_heartbeat"]).lt(svc["ttl"]))\
                .order_by('load')[0].default(
                    r.table('services')\
                        .get_all('trough-nodes', index='role')\
                        .order_by('load')[0].default({ })
                )
            ).run()
        post_url = 'http://%s:%s/provision' % (assignment['node'], self.sync_local_port)
        response = requests.post(post_url, json={'segment': segment_id, 'schema': schema})
        if response.status_code != 200:
            raise Exception('Received response from local write provisioner: %s: %s' % (response.status_code, response.text))
        result_dict = ujson.loads(response.text)
        return result_dict

    def promote_writable_segment_upstream(self, segment_id):
        # this function should make a call to the downstream server that holds the write lock

        # Consider use of this module: https://github.com/husio/python-sqlite3-backup
        # with pauses in between page copies to allow reads.
        # more reading on this topic here: https://www.sqlite.org/howtocorrupt.html
        assert False
    def list_schemas(self):
        gen = self.rethinker.table(Schema.table)['id'].run()
        result = list(gen)
        return result
    def get_schema(self, id):
        schema = Schema.load(self.rethinker, id)
        return schema
    def set_schema(self, id, schema=None):
        # create a document, insert/update it, overwriting document with id 'id'.
        created = False
        output = Schema.load(self.rethinker, id)
        if not output:
            output = Schema(self.rethinker, d={})
            created = True
        output.id = id
        output.sql = schema
        output.save()
        return (output, created)


# Local mode synchronizer.
class LocalSyncController(SyncController):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hostname = settings['HOSTNAME']

    def check_config(self):
        try:
            assert settings['HOSTNAME'], "HOSTNAME must be set, or I can't figure out my own hostname."
            assert settings['EXTERNAL_IP'], "EXTERNAL_IP must be set. We need to know which IP to use."
            assert settings['READ_PORT'], "READ_PORT must be set. We need to know the output port."
            assert settings['RETHINKDB_HOSTS'], "RETHINKDB_HOSTS must be set. Where can I contact RethinkDB on port 29015?"
        except AssertionError as e:
            sys.exit("{} Exiting...".format(str(e)))

    # def check_local_segment_is_fresh(self, segment):
    #     logging.info('Checking that segment %s is "fresh" in reference to HDFS.' % (segment.id,))
    #     try:
    #         snakebite_client = client.Client(settings['HDFS_HOST'], settings['HDFS_PORT'])
    #         listing = snakebite_client.ls([segment.remote_path()])
    #         listing = [item for item in listing]
    #         remote_mtime = listing[0]['modification_time'] / 1000
    #         local_mtime = os.path.getmtime(segment.local_path())
    #         if remote_mtime > local_mtime:
    #             logging.info('HDFS version (mtime: %s) is newer for segment %s (mtime: %s). Segment is "stale"' % (remote_mtime, segment.id, local_mtime))
    #             return False
    #         logging.info('Local copy is "fresh" for segment %s.' % (segment.id,))
    #         return True
    #     except Exception as e:
    #         logging.warning('Exception "%s" occurred while checking freshness for segment %s' % (e, segment.id))
    #     logging.warning('Segment %s considered "stale" with reference to HDFS version.' % segment.id)
    #     return False

    def copy_segment_from_hdfs(self, segment):
        logging.info('copying segment %s from HDFS...' % segment.id)
        source = [segment.remote_path()]
        destination = self.local_data
        # delete local file if it exists, otherwise surpress error
        with contextlib.suppress(FileNotFoundError):
            os.remove(segment.local_path())
        logging.info('running snakebite.Client.copyToLocal(%s, %s)' % (source, destination))
        snakebite_client = client.Client(settings['HDFS_HOST'], settings['HDFS_PORT'])
        for f in snakebite_client.copyToLocal(source, destination):
            if f.get('error'):
                logging.error('Error during HDFS copy: %s' % f['error'])
                raise Exception('Copying HDFS file %s to local destination %s produced an error: "%s"' % (source, destination, f['error']))
            logging.info('copied %s' % f)
            return True

    def heartbeat(self):
        logging.warning('Updating health check for "%s".' % self.hostname)
        # reset the countdown
        self.registry.heartbeat(pool='trough-nodes',
            node=self.hostname,
            ttl=round(self.sync_loop_timing * 4),
            available_bytes=self.storage_in_bytes
        )

    def decommission_writable_segment(self, segment, write_lock):
        logging.info('De-commissioning a writable segment: %s' % segment.id)
        writable_copy = segment.writable_copy()
        if writable_copy:
            self.services.unregister(writable_copy.get('id'))
        write_lock.release()

    def segment_name_from_path(self, path):
        return path.split("/")[-1].replace('.sqlite', '')

    def sync_segments(self):
        '''
        1. make a list of segment filenames assigned to this node
        2. make a list of remote segment file details as a hash table
        3. maks a list of local segment file details as a hash table
        4. for each item in the assignment list
            a. check that the file exists locally
            b. check that the local mtime is >= remote mtime
            c. if either check fails, enqueue the segment
            d. if both succeed, heartbeat (read and write if applicable)
        5. for each item in the failure queue
            a. copy the segment from HDFS
            b. heartbeat
            c. clean up write services
        '''
        # reset the TTL health check for this node. I still function!
        self.heartbeat()
        last_heartbeat = datetime.datetime.now()
        assignments = self.registry.segments_for_host(self.hostname)
        remote_listing = self.get_segment_file_list()
        logging.info('assembling remote file modification times...')
        remote_mtimes = { self.segment_name_from_path(file['path']): file['modification_time'] / 1000 for file in remote_listing }
        local_listing = os.listdir(self.local_data)
        logging.info('assembling local file modification times...')
        local_mtimes = {}
        for path in local_listing:
            try:
                local_mtimes[self.segment_name_from_path(path)] = os.stat(os.path.join(self.local_data, path)).st_mtime
            except:
                logging.warning('path %s appears to have moved during synchronization.')
        write_locks = { lock.segment: lock for lock in Lock.host_locks(self.rethinker, self.hostname) }
        stale_queue = []
        healthy_ids = []

        for segment in assignments:
            exists = segment.id in local_mtimes
            # TODO: what to do when remote copy is deleted?
            matches_hdfs = local_mtimes.get(segment.id, -1) >= remote_mtimes.get(segment.id, 0)
            if not exists or not matches_hdfs:
                stale_queue.append(segment)
                continue
            if write_locks.get(segment.id):
                write_id = 'trough-write:%s:%s' % (self.hostname, segment.id)
                logging.info('adding bulk write heartbeat for service id %s ...' % (write_id))
                healthy_ids.append(write_id)
            read_id = 'trough-read:%s:%s' % (self.hostname, segment.id)
            logging.info('adding bulk read heartbeat for service id %s...' % (read_id))
            healthy_ids.append(read_id)

        self.registry.bulk_heartbeat(healthy_ids)

        healthy_ids = []
        for segment in stale_queue:
            try:
                self.copy_segment_from_hdfs(segment)
            except:
                continue
            write_lock = segment.retrieve_write_lock()
            if write_lock:
                logging.info("Segment %s has a writable copy. It will be decommissioned in favor of the newer read-only copy from HDFS." % segment.id)
                self.decommission_writable_segment(segment, write_lock)
            read_id = 'trough-read:%s:%s' % (self.hostname, segment.id)
            logging.info('adding bulk read heartbeat for refreshed segment with service id %s...' % (read_id))
            healthy_ids.append(read_id)
            if datetime.datetime.now() - datetime.timedelta(seconds=round(settings['SYNC_LOOP_TIMING'] * 2)) > last_heartbeat:
                self.heartbeat()
                last_heartbeat = datetime.datetime.now()
                logging.info("copying down stale segments has exceeded 50% of the segments' lifetime. Copying will stop now, to allow services to check in.")
                break # start the sync loop over, allowing all segments to check in as healthy
        self.registry.bulk_heartbeat(healthy_ids)


    def provision_writable_segment(self, segment_id, schema='default'):
        # instantiate the segment
        segment = Segment(segment_id=segment_id,
            rethinker=self.rethinker,
            services=self.services,
            registry=self.registry,
            size=0)
        # get the current write lock if any # TODO: collapse the below into one query
        logging.info('retrieving write lock for segment %r', segment_id)
        lock_data = segment.retrieve_write_lock()
        if not lock_data:
            logging.info('acquiring write lock for segment %r', segment_id)
            lock_data = segment.acquire_write_lock()

        # TODO: spawn a thread for these?
        logging.info('heartbeating write service for segment %r', segment_id)
        trough_write_status = self.registry.heartbeat(pool='trough-write',
            segment=segment_id,
            node=self.hostname,
            port=self.write_port,
            url='http://%s:%s/?segment=%s' % (self.hostname, self.write_port, segment_id),
            ttl=round(self.sync_loop_timing * 4))

        logging.info('heartbeating read service for segment %r', segment_id)
        self.registry.heartbeat(pool='trough-read',
            segment=segment_id,
            node=self.hostname,
            port=self.read_port,
            url='http://%s:%s/?segment=%s' % (self.hostname, self.read_port, segment_id),
            ttl=round(self.sync_loop_timing * 4))

        # check that the file exists on the filesystem
        if not segment.local_segment_exists():
            # execute the provisioning sql file against the sqlite segment
            logging.info('provisioning local segment %r', segment_id)
            segment.provision_local_segment()
        logging.info('finished provisioning writable segment %r', segment_id)

        result_dict = {
            'write_url': trough_write_status['url'],
            'size': os.path.getsize(segment.local_path()),
            'schema': schema,
        }
        return result_dict

    def sync(self):
        '''
        "local" mode:
        - if not set up, 
            - set myself up as a host for a consul service as a read or write host depending on settings.
        - reset the countdown on my health check, if it exists
        - query consul for the assignment list for my hostname
            - check that we have a copy
            - check that the copy we have is the same or newer than hdfs copy
            - if either check fails:
                - copy file down from hdfs
                - set up a health check (TTL) for this segment, 2 * 'segment_timer'
            - touch segment health check
        '''
        self.sync_segments()

    def collect_garbage(self):
        assignments = set([item.id for item in self.registry.segments_for_host(self.hostname)])
        for item in os.listdir(self.local_data):
            if item.split(".")[0] not in assignments:
                path = os.path.join(self.local_data, item)
                logging.info("Deleting unassigned file: %s" % path)
                os.remove(path)


def get_controller(server_mode):
    logging.info('Connecting to Rethinkdb on: %s' % settings['RETHINKDB_HOSTS'])
    rethinker = doublethink.Rethinker(db="trough_configuration", servers=settings['RETHINKDB_HOSTS'])
    services = doublethink.ServiceRegistry(rethinker)
    registry = HostRegistry(rethinker=rethinker, services=services)
    init(rethinker)
    logging.info('Connecting to HDFS on: %s:%s' % (settings['HDFS_HOST'], settings['HDFS_PORT']))

    if server_mode:
        controller = MasterSyncController(
            rethinker=rethinker,
            services=services,
            registry=registry)
    else:
        controller = LocalSyncController(
            rethinker=rethinker,
            services=services,
            registry=registry)

    return controller
