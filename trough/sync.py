#!/usr/bin/env python3
import logging
import doublethink
import rethinkdb as r
from trough.settings import settings, init_worker
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
from hdfs3 import HDFileSystem
import threading
import tempfile

if settings['SENTRY_DSN']:
    try:
        import sentry_sdk
        sentry_sdk.init(settings['SENTRY_DSN'])
    except ImportError:
        logging.warning("'SENTRY_DSN' setting is configured but 'sentry_sdk' module not available. Install to use sentry.")


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

    # TODO these next two functions are stupidly specific to archive-it
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
        elif status_code in (0, -5003, -5004):
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
        logging.info("Committing %s assignments", self.length())
        self.rethinker.table('assignment').insert(self._queue).run()
        del self._queue[:]
    def length(self):
        return len(self._queue)

class UnassignmentQueue(AssignmentQueue):
    def commit(self):
        logging.info("Committing %s unassignments", self.length())
        ids = [item.id for item in self._queue]
        self.rethinker.table('assignment').get_all(*ids).delete().run()
        del self._queue[:]

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
        document["acquired_on"] = doublethink.utcnow()
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
        rethinker.table('services').index_create('role').run()
        rethinker.table('services').index_wait('segment').run()
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
        self.remote_path = remote_path
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
    def new_write_lock(self):
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
    def local_segment_exists(self):
        return os.path.isfile(self.local_path())
    def provision_local_segment(self, schema_sql):
        connection = sqlite3.connect(self.local_path())
        setup_connection(connection)
        cursor = connection.cursor()
        cursor.executescript(schema_sql)
        cursor.close()
        connection.commit()
        connection.close()
        logging.info('provisioned %s', self.local_path())
    def __repr__(self):
        return '<Segment:id=%r,local_path=%r>' % (self.id, self.local_path())

class HostRegistry(object):
    ''''''
    def __init__(self, rethinker, services):
        self.rethinker = rethinker
        self.services = services
        self.assignment_queue = AssignmentQueue(self.rethinker)
        self.unassignment_queue = UnassignmentQueue(self.rethinker)
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
            'assigned_on': doublethink.utcnow(),
            'remote_path': remote_path,
            'bytes': segment.size })
        logging.info('Adding "%s" to rethinkdb.' % (asmt))
        self.assignment_queue.enqueue(asmt)
        return asmt
    def unassign(self, assignment):
        self.unassignment_queue.enqueue(assignment)
    def commit_assignments(self):
        self.assignment_queue.commit()
    def commit_unassignments(self):
        self.unassignment_queue.commit()
    def segments_for_host(self, host):
        locks = Lock.host_locks(self.rethinker, host)
        segments = {lock.segment: Segment(segment_id=lock.segment, size=0, rethinker=self.rethinker, services=self.services, registry=self) for lock in locks}
        assignments = Assignment.host_assignments(self.rethinker, host)
        for asmt in assignments:
            segments[asmt.segment] = Segment(segment_id=asmt.segment, size=asmt.bytes, rethinker=self.rethinker, services=self.services, registry=self, remote_path=asmt.remote_path)
        logging.info('Checked for segments assigned to %s: Found %s segment(s)' % (host, len(segments)))
        return list(segments.values())

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
    def start(self):
        pass
    def check_config(self):
        raise Exception('Not Implemented')
    def ls_r(self, hdfs, path):
        for entry in hdfs.ls(path, detail=True):
            yield entry
            if entry['kind'] == 'directory':
                yield from self.ls_r(hdfs, entry['name'])
    def check_health(self):
        pass
    def get_segment_file_list(self):
        logging.info('Looking for *.sqlite in hdfs recursively under %s', self.hdfs_path)
        hdfs = HDFileSystem(host=self.hdfs_host, port=self.hdfs_port)
        return (entry for entry in self.ls_r(hdfs, self.hdfs_path)
                if entry['name'].endswith('.sqlite'))
    def list_schemas(self):
        gen = self.rethinker.table(Schema.table)['id'].run()
        result = list(gen)
        return result
    def get_schema(self, id):
        schema = Schema.load(self.rethinker, id)
        return schema
    def set_schema(self, id, sql):
        validate_schema_sql(sql)
        # create a document, insert/update it, overwriting document with id 'id'.
        created = False
        output = Schema.load(self.rethinker, id)
        if not output:
            output = Schema(self.rethinker, d={})
            created = True
        output.id = id
        output.sql = sql
        output.save()
        return (output, created)

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
                segment_id=file['name'].split('/')[-1].replace('.sqlite', ''),
                size=file['size'],
                remote_path=file['name'],
                rethinker=self.rethinker,
                services=self.services,
                registry=self.registry)
            segments.append(segment) # TODO: fix this per comment above.
        logging.info('assigning and balancing %r segments', len(segments))

        # host_ring_mapping will be e.g. { 'host1': { 'ring': 0, 'weight': 188921 }, 'host2': { 'ring': 0, 'weight': 190190091 }... }
        # the keys are node names, the values are array indices for the hash_rings variable (below)
        host_ring_mapping = Assignment.load(self.rethinker, "ring-assignments")
        if not host_ring_mapping:
            host_ring_mapping = Assignment(self.rethinker, {})
            host_ring_mapping.id = "ring-assignments"

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
            logging.info('pruning worker %r from pool (worker went offline?) [was: hash ring %s]', host, host_ring_mapping[host])
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
            host_ring = sorted(hash_rings, key=lambda ring: len(ring.get_nodes()))[0].id # TODO: this should be sorted by bytes, not raw # of nodes
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

        changed_assignments = 0
        # for each segment in segment list:
        for segment in segments:
            # if it's been over 80% of an election cycle since the last heartbeat, hold an election so we don't lose master status
            if datetime.datetime.now() - datetime.timedelta(seconds=0.8 * self.election_cycle) > last_heartbeat:
                if self.hold_election():
                    last_heartbeat = datetime.datetime.now()
                else:
                    return False
            logging.debug("Assigning segment [%s]", segment.id)
            # find position of segment in N hash rings, where N is the minimum number of assignments for this segment
            random.seed(segment.id) # (seed random so we always get the same sample of hash rings for this item)
            assigned_rings = random.sample(hash_rings, segment.minimum_assignments())
            logging.debug("Segment [%s] will use rings %s", segment.id, [s.id for s in assigned_rings])
            for ring in assigned_rings:
                # get the node for the key from hash ring, updating or creating assignments from corresponding entry in 'ring_assignments' as necessary
                assigned_node = ring.get_node(segment.id)
                dict_key = "%s-%s" % (ring.id, segment.id)
                assignment = ring_assignments.get(dict_key)
                logging.debug("Current assignment: '%s' New assignment: '%s'", assignment.node if assignment else None, assigned_node)
                if assignment is None or assignment.node != assigned_node:
                    changed_assignments += 1
                    logging.info("Segment [%s] will be assigned to host '%s' for ring [%s]", segment.id, assigned_node, ring.id)
                    if assignment:
                        logging.info("Removing old assignment to node '%s' for segment [%s]: (%s will be deleted)", assignment.node, segment.id, assignment)
                        self.registry.unassign(assignment)
                        del ring_assignments[dict_key]
                    ring_assignments[dict_key] = ring_assignments.get(dict_key, Assignment(self.rethinker, d={ 
                                                        'hash_ring': ring.id,
                                                        'node': assigned_node,
                                                        'segment': segment.id,
                                                        'assigned_on': doublethink.utcnow(),
                                                        'remote_path': segment.remote_path,
                                                        'bytes': segment.size }))
                    ring_assignments[dict_key]['node'] = assigned_node
                    ring_assignments[dict_key]['id'] = "%s:%s" % (ring_assignments[dict_key]['node'], ring_assignments[dict_key]['segment'])
                    self.registry.assignment_queue.enqueue(ring_assignments[dict_key])
        logging.info("%s assignments changed during this sync cycle.", changed_assignments)
        # commit assignments that were created or updated
        self.registry.commit_unassignments()
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

    def provision_writable_segment(self, segment_id, schema_id='default'):
        # the query below implements this algorithm:
        # - look up a write lock for the passed-in segment
        # - if the write lock exists
        # -   return it
        # - else
        # -   look up the set of readable copies of the segment
        # -   if readable copies exist
        # -     return the one with the lowest load
        # -   else (this is a new segment)
        # -     return the node (service entry with role trough-node) with lowest load
        #
        # the result is either:
        # - a 'lock' table entry table in case there is already a write lock
        #   for this segment
        # - a 'services' table entry in with role 'trough-read' in case the
        #   segment exists but is not under write
        # - a 'services' table entry with role 'trough-nodes' in case this is a
        #   new segment, in which case this is the node where we will provision
        #   the new segment
        assignment = self.rethinker.table('lock')\
            .get('write:lock:%s' % segment_id)\
            .default(r.table('services')\
                .get_all(segment_id, index='segment')\
                .filter({'role':'trough-read'})\
                .filter(lambda svc: r.now().sub(svc["last_heartbeat"]).lt(svc["ttl"]))\
                .order_by('load')[0].default(
                    r.table('services')\
                        .get_all('trough-nodes', index='role')\
                        .filter(lambda svc: r.now().sub(svc["last_heartbeat"]).lt(svc["ttl"]))\
                        .order_by('load')[0].default(None)
                )
            ).run()

        if not assignment:
            raise Exception('No healthy node to assign to')
        post_url = 'http://%s:%s/provision' % (assignment['node'], self.sync_local_port)
        json_data = {'segment': segment_id, 'schema': schema_id}
        response = requests.post(post_url, json=json_data)
        if response.status_code != 200:
            raise Exception('Received a %s response while provisioning segment "%s" on node %s:\n%r\nwhile posting %r to %r' % (response.status_code, segment_id, assignment['node'], response.text, ujson.dumps(json_data), post_url))
        result_dict = ujson.loads(response.text)
        return result_dict

    def promote_writable_segment_upstream(self, segment_id):
        # this function calls the downstream server that holds the write lock
        # if a lock exists, insert a flag representing the promotion into it, otherwise raise exception

        # forward the request downstream to actually perform the promotion
        write_lock = self.rethinker.table('lock').get('write:lock:%s' % segment_id).run()
        if not write_lock:
            raise Exception("Segment %s is not currently writable" % segment_id)
        post_url = 'http://%s:%s/promote' % (write_lock['node'], self.sync_local_port)
        json_data = {'segment': segment_id}
        logging.info('posting %s to %s', json.dumps(json_data), post_url)
        response = requests.post(post_url, json=json_data)
        if response.status_code != 200:
            raise Exception('Received a %s response while promoting segment "%s" to HDFS:\n%r\nwhile posting %r to %r' % (response.status_code, segment_id, response.text, ujson.dumps(json_data), post_url))
        response_dict = ujson.loads(response.content)
        if not 'remote_path' in response_dict:
            logging.warning('response json from downstream does not have remote_path?? %r', response_dict)
        return response_dict

def validate_schema_sql(sql):
    '''
    Schema sql is considered valid if it runs without error in an empty sqlite
    database.
    '''
    connection = sqlite3.connect(':memory:')
    connection.executescript(sql) # may raise exception
    connection.close()

# Local mode synchronizer.
class LocalSyncController(SyncController):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hostname = settings['HOSTNAME']
        self.read_id_tmpl = 'trough-read:%s:%%s' % self.hostname
        self.write_id_tmpl = 'trough-write:%s:%%s' % self.hostname
        self.healthy_service_ids = set()
        self.heartbeat_thread = threading.Thread(target=self.heartbeat_periodically_forever, daemon=True)

    def start(self):
        init_worker()
        self.heartbeat_thread.start()

    def heartbeat_periodically_forever(self):
        while True:
            start = time.time()
            try:
                healthy_service_ids = self.periodic_heartbeat()
                elapsed =  time.time() - start
                logging.info('heartbeated %s segments in %0.2f sec', len(healthy_service_ids), elapsed)
            except:
                elapsed =  time.time() - start
                logging.error('problem sending heartbeat', exc_info=True)
            time.sleep(self.sync_loop_timing - elapsed)

    def periodic_heartbeat(self):
        self.heartbeat()
        # make a copy for thread safety
        healthy_service_ids = list(self.healthy_service_ids)
        self.registry.bulk_heartbeat(healthy_service_ids)
        return healthy_service_ids

    def check_config(self):
        try:
            assert settings['HOSTNAME'], "HOSTNAME must be set, or I can't figure out my own hostname."
            assert settings['EXTERNAL_IP'], "EXTERNAL_IP must be set. We need to know which IP to use."
            assert settings['READ_PORT'], "READ_PORT must be set. We need to know the output port."
            assert settings['RETHINKDB_HOSTS'], "RETHINKDB_HOSTS must be set. Where can I contact RethinkDB on port 29015?"
        except AssertionError as e:
            sys.exit("{} Exiting...".format(str(e)))

    def check_health(self):
        assert self.heartbeat_thread.is_alive()

    def copy_segment_from_hdfs(self, segment):
        logging.debug('copying segment %r from HDFS path %r...', segment.id, segment.remote_path)
        assert segment.remote_path
        source = [segment.remote_path]
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_dest = os.path.join(tmpdir, "%s.sqlite" % segment.id)
            logging.debug('running snakebite.Client.copyToLocal(%r, %r)', source, tmp_dest)
            snakebite_client = client.Client(settings['HDFS_HOST'], settings['HDFS_PORT'])
            for f in snakebite_client.copyToLocal(source, tmp_dest):
                if f.get('error'):
                    raise Exception('Copying HDFS file %r to %r produced an error: %r' % (source, tmp_dest, f['error']))
                logging.debug('copying from hdfs succeeded, moving %s to %s', tmp_dest, segment.local_path())
                # clobbers segment.local_path if it already exists, which is what we want
                os.rename(tmp_dest, segment.local_path())
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
        write_lock.release()
        writable_copy = segment.writable_copy()
        if writable_copy:
            self.services.unregister(writable_copy.get('id'))

    def segment_id_from_path(self, path):
        return path.split("/")[-1].replace('.sqlite', '')

    def sync(self):
        '''
        assignments = list of segments assigned to this node
        local_segments = list of segments on local disk
        remote_segments = list of segments in hdfs
        segments_of_interest = set(assignments + local_segments)
        write_locks = list of locks assigned to this node

        for segment in self.healthy_service_ids:
            if segment not in segments_of_interest:
                discard write id from self.healthy_service_ids
                discard read id from self.healthy_service_ids

        for segment in segments_of_interest:
            if segment exists locally and is newer than hdfs:
                add read id to self.healthy_service_ids
                if segment in write_locks:
                    add write id to self.healthy_service_ids
            else: # segment does not exist locally or is older than hdfs:
                discard write id from self.healthy_service_ids
                discard read id from self.healthy_service_ids
                add to stale queue

        for segment in stale_queue:
            copy down from hdfs
            add read id to self.healthy_service_ids
            delete write lock from rethinkdb
        '''
        start = time.time()
        logging.info('sync starting')
        # { segment_id: Segment }
        my_segments = { segment.id: segment for segment in self.registry.segments_for_host(self.hostname) }
        remote_mtimes = {}  # { segment_id: mtime (long) }
        try:
            # iterator of dicts that look like this
            # {'last_mod': 1509406266, 'replication': 0, 'block_size': 0, 'name': '//tmp', 'group': 'supergroup', 'last_access': 0, 'owner': 'hdfs', 'kind': 'directory', 'permissions': 1023, 'encryption_info': None, 'size': 0}
            remote_listing = self.get_segment_file_list()
            for file in remote_listing:
                segment_id = self.segment_id_from_path(file['name'])
                remote_mtimes[segment_id] = file['last_mod']
            hdfs_up = True
        except Exception as e:
            logging.error('Error while listing files from HDFS', exc_info=True)
            logging.warning('PROCEEDING WITHOUT DATA FROM HDFS')
            hdfs_up = False
        logging.info('found %r segments in hdfs', len(remote_mtimes))
        # list of filenames
        local_listing = os.listdir(self.local_data)
        # { segment_id: mtime }
        local_mtimes = {}
        for path in local_listing:
            try:
                local_mtimes[self.segment_id_from_path(path)] = os.stat(os.path.join(self.local_data, path)).st_mtime
            except:
                logging.warning('%r gone since listing directory', path)
        logging.info('found %r segments on local disk', len(local_mtimes))
        # { segment_id: Lock }
        write_locks = { lock.segment: lock for lock in Lock.host_locks(self.rethinker, self.hostname) }
        writable_segments_found = len([1 for lock in write_locks if local_mtimes.get(lock)])
        logging.info('found %r writable segments on-disk and %r write locks in RethinkDB for host %r', writable_segments_found, len(write_locks), self.hostname)
        # list of segment id
        stale_queue = []

        segments_of_interest = set()
        segments_of_interest.update(my_segments.keys())
        segments_of_interest.update(local_mtimes.keys())

        count = 0
        for service_id in list(self.healthy_service_ids):
            segment_id = service_id.split(':')[-1]
            if segment_id not in segments_of_interest:
                self.healthy_service_ids.discard(service_id)
                logging.debug('discarded %r from healthy service ids because segment %r is gone from host %r', service_id, segment_id, self.hostname)
                count += 1
        logging.info('%r healthy service ids discarded on %r since last sync', count, self.hostname)

        for segment_id in segments_of_interest:
            if segment_id in local_mtimes and local_mtimes[segment_id] >= remote_mtimes.get(segment_id, 0):
                if (self.read_id_tmpl % segment_id) not in self.healthy_service_ids:
                    logging.debug('adding %r to healthy segment list', (self.read_id_tmpl % segment_id))
                self.healthy_service_ids.add(self.read_id_tmpl % segment_id)
                if segment_id in write_locks:
                    if (self.write_id_tmpl % segment_id) not in self.healthy_service_ids:
                        logging.debug('adding %r to healthy segment list', (self.write_id_tmpl % segment_id))
                    self.healthy_service_ids.add(self.write_id_tmpl % segment_id)
            else: # segment does not exist locally or is older than hdfs
                self.healthy_service_ids.discard(self.read_id_tmpl % segment_id)
                self.healthy_service_ids.discard(self.write_id_tmpl % segment_id)
                stale_queue.append(segment_id)

        if not hdfs_up:
            return

        num_processed = 0
        for segment_id in sorted(stale_queue, reverse=True):
            elapsed = time.time() - start
            if elapsed > self.sync_loop_timing:
                logging.debug('sync loop timed out after %0.1f sec', elapsed)
                break
            segment = my_segments.get(segment_id)
            num_processed += 1
            if not segment or not segment.remote_path:
                # There is a newer copy in hdfs but we are not assigned to
                # serve it. Do not copy down the new segment and do not release
                # the write lock. One of the assigned nodes will release the
                # write lock after copying it down, ensuring there is no period
                # of time when no one is serving the segment.
                continue
            if segment_id in local_mtimes:
                logging.info('replacing segment %r local copy (mtime=%s) from hdfs (mtime=%s)',
                             segment_id, datetime.datetime.fromtimestamp(local_mtimes[segment_id]),
                             datetime.datetime.fromtimestamp(remote_mtimes[segment_id]))
            else:
                logging.info('copying new segment %r from hdfs', segment_id)
            try:
                self.copy_segment_from_hdfs(segment)
            except Exception as e:
                logging.error('Error during HDFS copy of segment %r', segment_id, exc_info=True)
                continue
            self.healthy_service_ids.add(self.read_id_tmpl % segment_id)
            write_lock = segment.retrieve_write_lock()
            if write_lock:
                logging.info("Segment %s has a writable copy. It will be decommissioned in favor of the newer read-only copy from HDFS.", segment_id)
                self.decommission_writable_segment(segment, write_lock)
        logging.info('processed %s of %s stale segments in %0.1f sec', num_processed, len(stale_queue), time.time() - start)

    def provision_writable_segment(self, segment_id, schema_id='default'):
        # instantiate the segment
        segment = Segment(segment_id=segment_id,
            rethinker=self.rethinker,
            services=self.services,
            registry=self.registry,
            size=0)
        # get the current write lock if any # TODO: collapse the below into one query
        lock_data = segment.retrieve_write_lock()
        if lock_data:
            logging.info('retrieved existing write lock for segment %r', segment_id)
        else:
            lock_data = segment.new_write_lock()
            logging.info('acquired new write lock for segment %r', segment_id)

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

        # ensure that the file exists on the filesystem
        if not segment.local_segment_exists():
            # execute the provisioning sql file against the sqlite segment
            schema = self.get_schema(schema_id)
            if not schema:
                raise Exception('no such schema id=%r' % schema_id)
            logging.info('provisioning local segment %r', segment_id)
            segment.provision_local_segment(schema.sql)

        result_dict = {
            'write_url': trough_write_status['url'],
            'size': os.path.getsize(segment.local_path()),
            'schema': schema_id,
        }
        logging.info('finished provisioning writable segment %r', result_dict)
        return result_dict

    def do_segment_promotion(self, segment):
        import sqlitebck
        hdfs = HDFileSystem(host=self.hdfs_host, port=self.hdfs_port)
        with tempfile.NamedTemporaryFile() as temp_file:
            # "online backup" see https://www.sqlite.org/backup.html
            logging.info(
                    'backing up %s to %s', segment.local_path(),
                    temp_file.name)
            source = sqlite3.connect(segment.local_path())
            dest = sqlite3.connect(temp_file.name)
            sqlitebck.copy(source, dest)
            source.close()
            dest.close()
            logging.info(
                    'uploading %s to hdfs %s', temp_file.name,
                    segment.remote_path)
            hdfs.mkdir(os.path.dirname(segment.remote_path))
            # java hdfs convention, upload to foo._COPYING_
            tmp_name = '%s._COPYING_' % segment.remote_path
            hdfs.put(temp_file.name, tmp_name)

            # update mtime of local segment so that sync local doesn't think the
            # segment we just pushed to hdfs is newer (if it did, it would pull it
            # down and decommission its writable copy)
            # see https://webarchive.jira.com/browse/ARI-5713?focusedCommentId=110920#comment-110920
            os.utime(segment.local_path(), times=(time.time(), time.time()))

            # move existing out of the way if necessary (else mv fails)
            if hdfs.exists(segment.remote_path):
                hdfs.rm(segment.remote_path)

            # now move into place (does not update mtime)
            # returns False (does not raise exception) on failure
            result = hdfs.mv(tmp_name, segment.remote_path)
            assert result is True

            logging.info('Promoted writable segment %s upstream to %s', segment.id, segment.remote_path)

    def promote_writable_segment_upstream(self, segment_id):
        # load write lock, check segment is writable and not under promotion
        # update write lock to mark segment as being under promotion
        # get hdfs path from rethinkdb, use default if not set
        # push segment to hdfs
        # unset under_promotion flag
        # return response with hdfs path
        query = self.rethinker.table('lock')\
            .get('write:lock:%s' % segment_id)\
            .update({'under_promotion': True}, return_changes=True)
        result = query.run()
        try:
            write_lock = result['changes'][0]['new_val']
            assert write_lock['node'] == self.hostname
        except:
            if result['unchanged'] > 0:
                raise Exception("Segment %s is currently being copied upstream (write lock flag 'under_promotion' is set)" % segment_id)
            if result['skipped'] > 0:
                raise Exception("Segment %s is not currently writable" % segment_id)
            raise Exception("Unexpected result %r from rethinkdb query %r" % (result, query))

        try:
            try:
                assignment = self.rethinker.table('assignment').get_all(segment_id, index='segment')[0].run()
                remote_path = assignment['remote_path']
            except r.errors.ReqlNonExistenceError:
                remote_path = os.path.join(self.hdfs_path, segment_id[:3], '%s.sqlite' % segment_id)

            segment = Segment(
                    segment_id, size=-1, rethinker=self.rethinker,
                    services=self.services, registry=self.registry,
                    remote_path=remote_path)

            self.do_segment_promotion(segment)
        finally:
            self.rethinker.table('lock')\
                    .get('write:lock:%s' % segment_id)\
                    .update({'under_promotion': False}).run()
        return {'remote_path': remote_path}

    def collect_garbage(self):
        # for each segment file on local disk
        # - segment assigned to me should not be gc'd
        # - segment not assigned to me with healthy service count <= minimum
        #   should not be gc'd
        # - segment not assigned to me with healthy service count == minimum
        #   and no local healthy service entry should be gc'd
        # - segment not assigned to me with healthy service count > minimum
        #   and has local healthy service entry should be gc'd
        assignments = set(item.id for item in self.registry.segments_for_host(self.hostname))
        for filename in os.listdir(self.local_data):
            if not filename.endswith('.sqlite'):
                continue
            segment_id = filename[:-7]
            local_service_id = 'trough-read:%s:%s' % (self.hostname, segment_id)
            if segment_id not in assignments:
                segment = Segment(segment_id, 0, self.rethinker, self.services, self.registry)
                healthy_service_ids = {service['id'] for service in segment.readable_copies()}
                if local_service_id in healthy_service_ids:
                    healthy_service_ids.remove(local_service_id)
                    # re-check that the lock is not held by this machine before removing service
                    rechecked_lock = self.rethinker.table('lock').get(segment.id)
                    if len(healthy_service_ids) >= segment.minimum_assignments() \
                        and (rechecked_lock is None or rechecked_lock['node'] != self.hostname):
                        logging.info(
                                'segment %s has %s readable copies (minimum is %s) '
                                'and is not assigned to %s, removing %s from the '
                                'service registry',
                                segment_id, len(healthy_service_ids),
                                segment.minimum_assignments(), self.hostname,
                                local_service_id)
                        self.rethinker.table('services').get(local_service_id).delete().run()
                # re-check that the lock is not held by this machine before removing segment file
                rechecked_lock = self.rethinker.table('lock').get(segment.id)
                if len(healthy_service_ids) >= segment.minimum_assignments() \
                    and (rechecked_lock is None or rechecked_lock['node'] != self.hostname):
                    path = os.path.join(self.local_data, filename)
                    logging.info(
                            'segment %s now has %s readable copies (minimum '
                            'is %s) and is not assigned to %s, deleting %s',
                            segment_id, len(healthy_service_ids),
                            segment.minimum_assignments(), self.hostname,
                            path)
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
