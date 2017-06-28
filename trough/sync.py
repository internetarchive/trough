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

def healthy_services_query(rethinker, role):
    return rethinker.table('services').filter({"role": role}).filter(
        lambda svc: r.now().sub(svc["last_heartbeat"]) < svc["ttl"]
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
            self.id = "{host}:{segment}".format(host=self.host, segment=self.segment)
            self._pk = "id"
    @classmethod
    def table_create(cls, rr):
        rr.table_create(cls.table).run()
        rr.table(cls.table).index_create('segment').run()
        rr.table(cls.table).index_wait('segment').run()
    @classmethod
    def host_assignments(cls, rr, host):
        return (Assignment(rr, d=asmt) for asmt in rr.table(cls.table).between('%s:\x01' % host, '%s:\x7f' % host, right_bound="closed").run())
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

def ensure_tables(rethinker):
    Assignment.table_ensure(rethinker)
    Lock.table_ensure(rethinker)

class Segment(object):
    def __init__(self, segment_id, size, rethinker, services, registry):
        self.id = segment_id
        self.size = int(size)
        self.rethinker = rethinker
        self.services = services
        self.registry = registry
    def host_key(self, host):
        return "%s:%s" % (host, self.id)
    def all_copies(self):
        ''' returns the 'assigned' segment copies, whether or not they are 'up' '''
        return Assignment.segment_assignments(self.rethinker, self.id)
    def readable_copies_query(self):
        return healthy_services_query(self.rethinker, role='trough-read').filter({ 'segment': self.id })
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
        return self.services.available_services('trough-nodes')
    def hosts_exist(self):
        output = bool(self.get_hosts())
        logging.debug("Looking for hosts. Found: %s" % output)
        return output
    def total_bytes_for_node(self, node):
        for service in self.services.available_services('trough-nodes'):
            if service['node'] == node:
                return service.get('available_bytes')
        raise Exception('Could not find node "%s"' % node)
    def host_load(self):
        logging.info('Beginning Host Load Calculation...')
        output = []
        for host in self.get_hosts():
            logging.info('Working on host %s' % host)
            assigned_bytes = sum([assignment.bytes for assignment in Assignment.host_assignments(self.rethinker, host['node'])])
            logging.info('Found %s bytes assigned to host %s' % (assigned_bytes, host))
            total_bytes = self.total_bytes_for_node(host['node'])
            logging.info('Total bytes for node: %s' % total_bytes)
            total_bytes = 0 if total_bytes in ['null', None] else int(total_bytes)
            output.append({
                'node': host['node'],
                'remaining_bytes': total_bytes - assigned_bytes,
                'assigned_bytes': assigned_bytes,
                'total_bytes': total_bytes,
                'load_ratio': assigned_bytes / total_bytes,
            })
        return output
    def min_acceptable_load_ratio(self, hosts, largest_segment_size):
        ''' the minimum acceptable ratio is the average load ratio minus the accepable load deviation.
        the acceptable load deviation is 1.5 * (the ratio of (largest segment : the average byte load of the nodes))'''
        # if there are no segments, (the size of the largest one is zero, don't move any segments)
        if largest_segment_size == 0:
            return 0
        segment_size_sum = 0
        average_load_ratio = sum([host['load_ratio'] for host in hosts]) / len(hosts)
        average_byte_load = sum([host['assigned_bytes'] for host in hosts]) / len(hosts)
        average_capacity = sum([host['total_bytes'] for host in hosts]) / len(hosts)
        acceptable_load_deviation = min((largest_segment_size / average_capacity), average_load_ratio)
        min_acceptable_load = average_load_ratio - acceptable_load_deviation
        return min_acceptable_load
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
        self.services.heartbeat(doc)
    def assign(self, hostname, segment, remote_path):
        logging.info("Assigning segment: %s to '%s'" % (segment.id, hostname))
        asmt = Assignment(self.rethinker, d={ 
            'host': hostname,
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
        self.found_hosts = False

        self.hostname = settings['HOSTNAME']
        self.external_ip = settings['EXTERNAL_IP']
        self.rethinkdb_hosts = settings['RETHINKDB_HOSTS']

        self.hdfs_path = settings['HDFS_PATH']
        self.hdfs_host = settings['HDFS_HOST']
        self.hdfs_port = settings['HDFS_PORT']

        self.election_cycle = settings['ELECTION_CYCLE']
        self.sync_port = settings['SYNC_PORT']
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
    def check_config(self):
        try:
            assert settings['HDFS_PATH'], "HDFS_PATH must be set, otherwise I don't know where to look for sqlite files."
            assert settings['HDFS_HOST'], "HDFS_HOST must be set, or I can't communicate with HDFS."
            assert settings['HDFS_PORT'], "HDFS_PORT must be set, or I can't communicate with HDFS."
            assert settings['ELECTION_CYCLE'] > 0, "ELECTION_CYCLE must be greater than zero. It governs the number of seconds in a sync master election period."
            assert settings['HOSTNAME'], "HOSTNAME must be set, or I can't figure out my own hostname."
            assert settings['EXTERNAL_IP'], "EXTERNAL_IP must be set. We need to know which IP to use."
            assert settings['SYNC_PORT'], "SYNC_PORT must be set. We need to know the output port."
            assert settings['RETHINKDB_HOSTS'], "RETHINKDB_HOSTS must be set. Where can I contact RethinkDB on port 29015?"
        except AssertionError as e:
            sys.exit("{} Exiting...".format(str(e)))

    def hold_election(self):
        logging.info('Holding Sync Master Election...')
        candidate = { 
            "id": "trough-sync-master",
            "node": self.hostname,
            "port": self.sync_port,
            "url": "http://%s:%s/" % (self.hostname, self.sync_port),
            "ttl": self.election_cycle + self.sync_loop_timing * 4,
        }
        sync_master = self.services.unique_service('trough-sync-master', candidate=candidate)
        if sync_master.get('node') == self.hostname:
            # 'touch' the ttl check for sync master
            logging.info('Still the master. I will check again in %ss' % self.election_cycle)
            return True
        logging.info('I am not the master. I will check again in %ss' % self.election_cycle)
        return False

    def wait_to_become_leader(self):
        # hold an election every self.election_cycle seconds
        self.leader = self.hold_election()
        while not self.leader:
            self.leader = self.hold_election()
            if not self.leader:
                time.sleep(self.election_cycle)

    def wait_for_hosts(self):
        while not self.found_hosts:
            logging.info('Waiting for hosts to join cluster. Sleep period: %ss' % self.host_check_wait_period)
            self.found_hosts = self.registry.hosts_exist()
            time.sleep(self.host_check_wait_period)

    def assign_segments(self):
        logging.info('Assigning segments...')
        for file in self.get_segment_file_list():
            local_part = file['path'].split('/')[-1]
            local_part = local_part.replace('.sqlite', '')
            segment = Segment(segment_id=local_part, rethinker=self.rethinker, services=self.services, registry=self.registry, size=file['length'])
            assignment_count = len([1 for cpy in segment.all_copies()])
            logging.info("Checking segment [%s]:  %s assignments of %s minimum assignments." % (segment.id, assignment_count, segment.minimum_assignments()))
            if assignment_count < segment.minimum_assignments():
                host_load = self.registry.host_load()
                logging.info('Calculated Host Load: %s' % host_load)
                emptiest_host = sorted(host_load, key=lambda host: host['assigned_bytes'])[0]
                # assign the byte count of the file to a key named, e.g. /hostA/segment
                self.registry.assign(emptiest_host['node'], segment, remote_path=file['path'])
            elif assignment_count > segment.minimum_assignments():
                # If we find too many 'up' copies
                if len([1 for cpy in segment.readable_copies()]) > segment.minimum_assignments():
                    # delete the copy with the lowest 'assigned_on', which records the
                    # date on which assignments are created.
                    assignments = segment.all_copies()
                    assignments = sorted(assignments, key=lambda record: record['assigned_on'])
                    # remove the assignment
                    assignments[0].unassign()
        self.registry.commit_assignments()

    def rebalance_hosts(self):
        logging.info('Rebalancing Hosts...')
        hosts = self.registry.host_load()
        largest_segment_size = 0
        segment_file_list = self.get_segment_file_list()
        segment_count = 0
        for segment in segment_file_list:
            segment_count += 1
            if segment['length'] > largest_segment_size:
                largest_segment_size = segment['length']
        logging.info('Found a largest segment size of %s bytes' % largest_segment_size)
        min_acceptable_load = self.registry.min_acceptable_load_ratio(hosts, largest_segment_size)
        # filter out any hosts that have very little free space *and* are underloaded. They can't be fixed, most likely.
        nonfull_nonunderloaded_hosts = [host for host in hosts if not (host['load_ratio'] < min_acceptable_load and host['remaining_bytes'] <= largest_segment_size)]
        # sort hosts descending
        sorted_hosts = sorted(nonfull_nonunderloaded_hosts, key=lambda host: host['load_ratio'], reverse=True)
        logging.info("Non-full hosts which are underloaded: %s" % sorted_hosts)
        queue = []
        assignment_cache = {}
        last_reassignment = None
        counter = 0
        while sorted_hosts[-1]['load_ratio'] < min_acceptable_load and len(sorted_hosts) >= 2 and counter < segment_count:
            counter += 1
            logging.info("Looping while the least-loaded host's load ratio (%s) is less than %s and there are at least 2 hosts to assign from/to (there are %s)" % (sorted_hosts[-1]['load_ratio'], min_acceptable_load, sorted_hosts))
            # get the top-loaded host
            top_host = sorted_hosts[0]
            underloaded_host = sorted_hosts[-1]
            # if we haven't seen this host before, cache a list of its segments
            if top_host['node'] not in assignment_cache:
                assignment_cache[top_host['node']] = [asmt for asmt in Assignment.host_assignments(self.rethinker, top_host['node'])]
                # shuffle segment order so when we .pop() it selects a random segment.
                random.shuffle(assignment_cache[top_host['node']])
            # pick a segment assigned to the top-loaded host
            #print("assignment_cache[top_host['node']]: %s " % assignment_cache[top_host['node']])
            reassign_from = assignment_cache[top_host['node']].pop()
            segment_name = reassign_from.segment
            reassign_to = Assignment(self.rethinker, d={
                'host': underloaded_host['node'],
                'segment': reassign_from.segment,
                'assigned_on': r.now(),
                'remote_path': reassign_from.remote_path,
                'bytes': reassign_from.bytes })
            reassignment = (reassign_from, reassign_to)
            segment_bytes = reassign_from.bytes
            # if our last reassignment is the same as the current reassignment, there's only one segment on this host. Pop it.
            if reassign_from == last_reassignment:
                hosts.pop(0)
                continue
            last_reassignment = reassign_from
            # if this segment is already assigned to the bottom loaded host, put it back in at the top of the list and loop.
            if Assignment.load(self.rethinker, "%s:%s" % (reassign_to.host, reassign_to.segment)):
                assignment_cache[top_host['node']].insert(0, reassign_from)
                continue
            # enqueue segment
            queue.append(reassignment)
            # recalculate host load
            top_host['assigned_bytes'] -= segment_bytes
            top_host['remaining_bytes'] += segment_bytes
            top_host['load_ratio'] = top_host['assigned_bytes'] / top_host['total_bytes']
            underloaded_host['assigned_bytes'] += segment_bytes
            underloaded_host['remaining_bytes'] -= segment_bytes
            underloaded_host['load_ratio'] = underloaded_host['assigned_bytes'] / underloaded_host['total_bytes']
            sorted_hosts.sort(key=lambda host: host['load_ratio'], reverse=True)
        # perform the queued reassignments. We set a key value pair in consul to assign.
        for reassignment in queue:
            reassignment[1].save()

    def sync(self):
        ''' 
        "server" mode:
        - if I am not the leader, poll forever
        - if there are hosts to assign to, poll forever.
        - for entire list of segments that match pattern in REMOTE_DATA setting:
            - check consul to make sure each item is assigned to a worker
            - if it is not assigned:
                - assign it, based on the available quota on each worker
            - if the number of assignments for this segment are greater than they should be, and all copies are 'up':
                - unassign the copy with the lowest assignment index
        - for list of hosts:
            - if this host meets a "too empty" metric
                - loop over the segments
                - add extra assignments to the "too empty" host in a ratio of segments which corresponds to the delta from the average load.
        '''
        self.found_hosts = False
        # loops indefinitely waiting to become leader.
        self.wait_to_become_leader()
        # loops indefinitely waiting for hosts to hosts to which to assign segments
        self.wait_for_hosts()
        # assign each segment we found in our HDFS file listing
        self.assign_segments()
        # rebalance the hosts in case any new servers join the cluster
        self.rebalance_hosts()

    def wait_for_write_lock(self, segment_id):
        lock = None
        while not lock:
            lock = Lock.acquire(self.rethinker, pk='master/%s' % segment_id)
            print(lock)
        return lock

    def provision_writable_segment(self, segment_id):
        # to protect against querying the non-leader
        # get the hostname of the leader
        # if not my hostname, raise exception
        # acquire a lock for the process of provisioning
        # with lock:
        lock = self.wait_for_write_lock(segment_id)
        segment = Segment(segment_id=segment_id,
            rethinker=self.rethinker,
            services=self.services,
            registry=self.registry, size=0)
        assigned_host = segment.retrieve_write_lock()
        readable_copies = [copy for copy in segment.readable_copies()]
        # if the requested segment has no writable copies:
        if not assigned_host:
            all_hosts = self.registry.get_hosts()
            assigned_host = random.choice(readable_copies) if readable_copies else random.choice(all_hosts)
        if not readable_copies:
            self.registry.assign(assigned_host['node'], segment, remote_path=None)
        # make request to node to complete the local sync
        post_url = 'http://%s:%s/' % (assigned_host['node'], self.sync_port)
        requests.post(post_url, segment_id)
        # explicitly release provisioning lock
        lock.release()
        # return an http endpoint for POSTs
        return "http://%s:%s/?segment=%s" % (assigned_host['node'], self.write_port, segment_id)

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
                logging.error('Error: %s' % f['error'])
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
        segment.writable_copy().id
        logging.info('De-commissioning a writable segment: %s' % segment.id)
        self.services.unregister(segment.writable_copy().id)
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
        assignments = self.registry.segments_for_host(self.hostname)
        remote_listing = self.get_segment_file_list()
        logging.info('assembling remote file modification times...')
        remote_mtimes = { self.segment_name_from_path(file['path']): file['modification_time'] / 1000 for file in remote_listing }
        local_listing = os.listdir(self.local_data)
        logging.info('assembling local file modification times...')
        local_mtimes = { self.segment_name_from_path(path): os.stat(os.path.join(self.local_data, path)).st_mtime for path in local_listing }
        write_locks = { lock.segment: lock for lock in Lock.host_locks(self.rethinker, self.hostname) }
        stale_queue = []
        segment_health_ttl = self.sync_loop_timing * 4

        for segment in assignments:
            exists = segment.id in local_mtimes
            # TODO: what to do when remote copy is deleted?
            matches_hdfs = local_mtimes.get(segment.id, -1) >= remote_mtimes.get(segment.id, 0)
            if not exists or not matches_hdfs:
                stale_queue.append(segment)
                continue
            if write_locks.get(segment.id):
                logging.info('write heartbeat for segment %s...' % (segment.id))
                self.registry.heartbeat(pool='trough-write',
                    segment=segment.id,
                    node=self.hostname,
                    port=self.write_port,
                    url='http://%s:%s/?segment=%s' % (self.hostname, self.write_port, segment.id),
                    ttl=segment_health_ttl)
            logging.info('read heartbeat for segment %s...' % (segment.id))
            self.registry.heartbeat(pool='trough-read',
                segment=segment.id,
                node=self.hostname,
                port=self.read_port,
                url='http://%s:%s/?segment=%s' % (self.hostname, self.read_port, segment.id),
                ttl=segment_health_ttl)

        for segment in stale_queue:
            if write_locks.get(segment.id):
                logging.info("Segment %s has a writable copy. It will be decommissioned in favor of the newer read-only copy from HDFS." % segment.id)
                self.decommission_writable_segment(segment, write_locks[segment.id])
            self.copy_segment_from_hdfs(segment)
            logging.info('read heartbeat for refreshed segment %s...' % (segment.id))
            self.registry.heartbeat(pool='trough-read',
                segment=segment.id,
                node=self.hostname,
                port=self.read_port,
                url='http://%s:%s/?segment=%s' % (self.hostname, self.read_port, segment.id),
                ttl=segment_health_ttl)

    def provision_writable_segment(self, segment_id):
        # instantiate the segment
        segment = Segment(segment_id=segment_id,
            rethinker=self.rethinker,
            services=self.services,
            registry=self.registry,
            size=0)
        # get the current write lock if any
        lock_data = segment.retrieve_write_lock()
        if not lock_data:
            lock_data = segment.acquire_write_lock()

        self.registry.heartbeat(pool='trough-write',
            segment=segment_id,
            node=self.hostname,
            port=self.write_port,
            url='http://%s:%s/?segment=%s' % (self.hostname, self.write_port, segment_id),
            ttl=round(self.sync_loop_timing * 4))

        self.registry.heartbeat(pool='trough-read',
            segment=segment_id,
            node=self.hostname,
            port=self.read_port,
            url='http://%s:%s/?segment=%s' % (self.hostname, self.read_port, segment_id),
            ttl=round(self.sync_loop_timing * 4))

        # check that the file exists on the filesystem
        if not segment.local_segment_exists():
            # execute the provisioning sql file against the sqlite segment
            segment.provision_local_segment()

    def sync(self):
        '''
        "local" mode:
        - if not set up, 
            - set myself up as a host for a consul service as a read or write host depending on settings.
        - reset the countdown on my health check, if it exists
        - figure out what my data limit is (look it up from settings), persisting to consul.
        - query consul for the assignment list for my hostname
        - start 'timer'
        - for each item in the list:
            - check that we have a copy
            - check that the copy we have matches the byte size in hdfs
            - if either check fails:
                - copy file down from hdfs
                - set up a health check (TTL) for this segment, 2 * 'segment_timer'
            - touch segment health check
        - end 'timer'
        - set up a health check (TTL) for myself, 2 * 'timer'
        '''
        # reset the TTL health check for this node. I still function!
        self.heartbeat()
        # sync my local segments with HDFS
        self.sync_segments()

def get_controller(server_mode):
    logging.info('Connecting to Rethinkdb on: %s' % settings['RETHINKDB_HOSTS'])
    rethinker = doublethink.Rethinker(db="trough_configuration", servers=settings['RETHINKDB_HOSTS'])
    services = doublethink.ServiceRegistry(rethinker)
    registry = HostRegistry(rethinker=rethinker, services=services)
    ensure_tables(rethinker)
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
