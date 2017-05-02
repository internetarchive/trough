#!/usr/bin/env python3
import logging
import doublethink
import rethinkdb as r
from trough.settings import settings
from snakebite.client import Client
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

class AssignmentQueue:
    def __init__(self, rethinker):
        self._queue = []
        self.rethinker = rethinker
    def enqueue(self, item):
        self._queue.append(item)
        if self.length() >= 1000:
            self.commit()
    def commit(self):
        self._queue
        self.rethinker.table('assignment').insert(self._queue).run();
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
        return self.rr.table(self.table).get(self.id).delete()

class Lock(doublethink.Document):
    @classmethod
    def acquire(cls, rr, pk, document={}):
        document["pk"] = pk
        document["acquired_on"] = r.now()
        try:
            rr.table(cls.table).insert(document)
        except Exception as e:
            raise LockException('Unable to acquire a lock for %s' % pk)
        return cls(rr, d=document)
    def release(self):
        return self.rr.table(self.table, read_mode='majority').get(self.pk).delete()

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
    def readable_copies(self):
        '''returns the 'up' copies of this segment to read from, per consul.'''
        return (copy for copy in self.services.available_services('trough-read') if copy['segment'] == self.id)
    def writable_copies(self):
        '''returns the 'up' copies of this segment to write to, per consul.'''
        return (copy for copy in self.services.available_services('trough-write') if copy['segment'] == self.id)
    def is_assigned_to_host(self, host):
        return bool(Assignment.load(self.rethinker, self.host_key(host)))
    def minimum_assignments(self):
        '''This function should return the minimum number of assignments which is acceptable for a given segment.'''
        if hasattr(settings['MINIMUM_ASSIGNMENTS'], "__call__"):
            return settings['MINIMUM_ASSIGNMENTS'](self.id)
        else:
            return settings['MINIMUM_ASSIGNMENTS']
    def acquire_write_lock(self, host):
        '''Raises exception if required parameter "host" is not provided. Raises exception if lock exists.'''
        return Lock.acquire(self.rethinker, pk='write:lock:%s' % segment_id, document={})
    def retrieve_write_lock(self):
        '''Returns None or dict. Can be used to evaluate whether a lock exists and, if so, which host holds it.'''
        return Lock.load(self.rethinker, pk='write:lock:%s' % segment_id)
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
        cursor = connection.cursor()
        with open(settings['SEGMENT_INITIALIZATION_SQL'], 'r') as script:
            for query in script:
                cursor.execute(query)
        cursor.close()
        connection.commit()
        connection.close()

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
        return 0
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
    def heartbeat(self, pool=None, node=None, heartbeat_interval=None, **doc):
        if None in [pool, node, heartbeat_interval]:
            raise Exception('"pool", "node" and "heartbeat_interval" are required arguments.')
        doc['id'] = "%s:%s:%s" % (pool, node, doc.get('segment'))
        logging.info("Setting Heartbeat ID to [%s]" % doc['id'])
        doc['role'] = pool
        doc['node'] = node
        doc['heartbeat_interval'] = heartbeat_interval
        doc['load'] = os.getloadavg()[1] # load average over last 5 mins
        logging.info('Heartbeat: role[%s] node[%s] at IP %s:%s with heartbeat interval %s' % (pool, node, node, doc.get('port'), heartbeat_interval))
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
    def segments_for_host(self, host):
        assignments = Assignment.host_assignments(self.rethinker, host)
        segments = [Segment(segment_id=asmt.segment, size=asmt.bytes, rethinker=self.rethinker, services=self.services, registry=self) for asmt in assignments]
        logging.info('Checked for segments assigned to %s: Found %s segment(s)' % (host, len(segments)))
        return segments
    def commit_assignments(self):
        self.assignment_queue.commit()

# Base class, not intended for use.
class SyncController:
    def __init__(self, rethinker=None, services=None, registry=None, snakebite_client=None):
        self.rethinker = rethinker
        self.services = services
        self.registry = registry
        self.snakebite_client = snakebite_client
        self.leader = False
        self.found_hosts = False
    def check_config(self):
        raise Exception('Not Implemented')
    def get_segment_file_list(self):
        logging.info('Getting segment list...')
        return self.snakebite_client.ls([settings['HDFS_PATH']])
    def get_segment_file_size(self, segment):
        sizes = [file['length'] for file in self.snakebite_client.ls([segment.remote_path()])]
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
            assert settings['RETHINKDB_HOSTS'], "RETHINKDB_HOST must be set. Where can I contact RethinkDB on port 29015?"
        except AssertionError as e:
            sys.exit("{} Exiting...".format(str(e)))

    def hold_election(self):
        logging.info('Holding Sync Master Election...')
        candidate = { 
            "id": "trough-sync-master",
            "node": settings['HOSTNAME'],
            "heartbeat_interval": settings['ELECTION_CYCLE'] + settings['SYNC_LOOP_TIMING'] * 2,
        }
        sync_master = self.services.unique_service('trough-sync-master', candidate=candidate)
        if sync_master.get('node') == settings['HOSTNAME']:
            # 'touch' the ttl check for sync master
            logging.info('Still the master. I will check again in %ss' % settings['ELECTION_CYCLE'])
            return True
        logging.info('I am not the master. I will check again in %ss' % settings['ELECTION_CYCLE'])
        return False

    def wait_to_become_leader(self):
        # hold an election every settings['ELECTION_CYCLE'] seconds
        self.leader = self.hold_election()
        while not self.leader:
            self.leader = self.hold_election()
            if not self.leader:
                time.sleep(settings['ELECTION_CYCLE'])

    def wait_for_hosts(self):
        while not self.found_hosts:
            logging.info('Waiting for hosts to join cluster. Sleep period: %ss' % settings['HOST_CHECK_WAIT_PERIOD'])
            self.found_hosts = self.registry.hosts_exist()
            time.sleep(settings['HOST_CHECK_WAIT_PERIOD'])

    def assign_segments(self):
        logging.info('Assigning segments...')
        for file in self.get_segment_file_list():
            local_part = file['path'].split('/')[-1]
            local_part = local_part.replace('.sqlite', '')
            segment = Segment(segment_id=local_part, rethinker=self.rethinker, services=self.services, registry=self.registry, size=file['length'])
            assignment_count = len([1 for cpy in segment.all_copies()])
            logging.info("Checking segment [%s]:  %s assignments of %s minimum assignments." % (segment.id, assignment_count, segment.minimum_assignments()))
            if not assignment_count >= segment.minimum_assignments():
                host_load = self.registry.host_load()
                logging.info('Calculated Host Load: %s' % host_load)
                emptiest_host = sorted(host_load, key=lambda host: host['assigned_bytes'])[0]
                # assign the byte count of the file to a key named, e.g. /hostA/segment
                self.registry.assign(emptiest_host['node'], segment, remote_path=file['path'])
            else:
                # If we find too many 'up' copies
                if len([1 for cpy in segment.readable_copies()]) > segment.minimum_assignments():
                    # delete the copy with the lowest 'assigned_on', which records the
                    # date on which assignments are created.
                    assignments = segment.all_copies()
                    assignments = sorted(assignments, key=lambda record: record['assigned_on'])
                    # remove the assignment
                    assignments[0].unassign()
            writable_copies = [copy for copy in segment.writable_copies()]
            if writable_copies:
                logging.info("Segment %s has a writable copy. It will be decommissioned in favor of the read-only copy from HDFS." % segment.id)
                self.decommission_writable_segment(writable_copies[0].get('id'))
        self.registry.commit_assignments()

    def rebalance_hosts(self):
        logging.info('Rebalancing Hosts...')
        hosts = self.registry.host_load()
        largest_segment_size = 0
        segment_file_list = self.get_segment_file_list()
        for segment in segment_file_list:
            if segment['length'] > largest_segment_size:
                largest_segment_size = segment['length']
        min_acceptable_load = self.registry.min_acceptable_load_ratio(hosts, largest_segment_size)
        # filter out any hosts that have very little free space *and* are underloaded. They can't be fixed, most likely.
        nonfull_nonunderloaded_hosts = [host for host in hosts if not (host['load_ratio'] < min_acceptable_load and host['remaining_bytes'] <= largest_segment_size)]
        # sort hosts descending
        sorted_hosts = sorted(nonfull_nonunderloaded_hosts, key=lambda host: host['load_ratio'], reverse=True)
        queue = []
        assignment_cache = {}
        last_reassignment = None
        while sorted_hosts[-1]['load_ratio'] < min_acceptable_load and len(sorted_hosts) >= 2:
            # get the top-loaded host
            top_host = sorted_hosts[0]
            underloaded_host = sorted_hosts[-1]
            # if we haven't seen this host before, cache a list of its segments
            if top_host['node'] not in assignment_cache:
                assignment_cache[top_host['node']] = Assignment.host_assignments(self.rethinker, top_host['node'])
                # shuffle segment order so when we .pop() it selects a random segment.
                random.shuffle(assignment_cache[top_host['node']])
            # pick a segment assigned to the top-loaded host
            reassign_from = assignment_cache[top_host['node']].pop()
            segment_name = reassign_from.segment
            reassign_to = Assignment(rr, d={
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
            if Assignment.load(self.rethinker, "%s:%s" % reassign_to.host, reassign_to.segment):
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
        writable_copies = [copy for copy in segment.writable_copies()]
        readable_copies = [copy for copy in segment.readable_copies()]
        # if the requested segment has no writable copies:
        if len(writable_copies) == 0:
            all_hosts = self.registry.get_hosts()
            assigned_host = random.choice(readable_copies) if readable_copies else random_choice(all_hosts)
            # make request to node to complete the local sync
            post_url = 'http://%s:%s/' % (assigned_host, settings['SYNC_PORT'])
            requests.post(post_url, segment_id)
            # create a new consul service
            self.registry.heartbeat(pool='trough-write',
                segment=segment_id,
                node=assigned_host,
                port=settings['WRITE_PORT'],
                heartbeat_interval=round(settings['SYNC_LOOP_TIMING'] * 2))
        else:
            assigned_host = writable_copies[0]
        # explicitly release provisioning lock (do nothing)
        lock.release()
        # return an http endpoint for POSTs
        return "http://%s:%s/?segment=%s" % (assigned_host, settings['WRITE_PORT'], segment_id)

    def decommission_writable_segment(self, service_id):
        logging.info('De-commissioning a writable segment: %s' % service_id)
        self.services.unregister(service_id)

# Local mode synchronizer.
class LocalSyncController(SyncController):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hostname = settings['HOSTNAME']

    def check_config(self):
        try:
            assert settings['HOSTNAME'], "HOSTNAME must be set, or I can't figure out my own hostname."
            assert settings['EXTERNAL_IP'], "EXTERNAL_IP must be set. We need to know which IP to use."
            assert settings['READ_PORT'], "SYNC_PORT must be set. We need to know the output port."
            assert settings['RETHINKDB_HOSTS'], "RETHINKDB_HOST must be set. Where can I contact RethinkDB on port 29015?"
        except AssertionError as e:
            sys.exit("{} Exiting...".format(str(e)))

    def check_segment_matches_hdfs(self, segment):
        logging.info('Checking that segment %s matches its byte count in HDFS.' % (segment.id,))
        try:
            logging.info('HDFS recorded size: %s' % segment.size)
            local_size = os.path.getsize(segment.local_path())
            logging.info('local size: %s' % local_size)
            if segment.size == local_size:
                logging.info('Byte counts match.')
                return True
        except Exception as e:
            logging.warning('Exception occurred while checking byte count match for %s' % segment.id)
        logging.warning('Byte counts do not match HDFS for segment %s' % segment.id)
        return False

    def copy_segment_from_hdfs(self, segment):
        logging.info('copying segment %s from HDFS...' % segment.id)
        source = [os.path.join(settings['HDFS_PATH'], "%s.sqlite" % segment.id)]
        destination = settings['LOCAL_DATA']
        logging.info('running snakebite.Client.copyToLocal(%s, %s)' % (source, destination))
        for f in self.snakebite_client.copyToLocal(source, destination):
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
            heartbeat_interval=round(settings['SYNC_LOOP_TIMING'] * 2),
            available_bytes=settings['STORAGE_IN_BYTES']
        )

    def sync_segments(self):
        segment_health_ttl = settings['SYNC_LOOP_TIMING'] * 2
        for segment in self.registry.segments_for_host(self.hostname):
            exists = segment.local_segment_exists()
            matches_hdfs = self.check_segment_matches_hdfs(segment)
            if not exists or not matches_hdfs:
                self.copy_segment_from_hdfs(segment)
            logging.info('registering segment %s...' % (segment.id))
            self.registry.heartbeat(pool='trough-read',
                segment=segment.id,
                node=self.hostname,
                heartbeat_interval=segment_health_ttl)

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
            lock_data = segment.acquire_write_lock(settings['HOSTNAME'])
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
    logging.info('Connecting to HDFS on: %s:%s' % (settings['HDFS_HOST'], settings['HDFS_PORT']))
    snakebite_client = Client(settings['HDFS_HOST'], settings['HDFS_PORT'])
    Assignment.table_ensure(rethinker)
    Lock.table_ensure(rethinker)

    if server_mode:
        controller = MasterSyncController(
            rethinker=rethinker,
            services=services,
            registry=registry,
            snakebite_client=snakebite_client)
    else:
        controller = LocalSyncController(
            rethinker=rethinker,
            services=services,
            registry=registry,
            snakebite_client=snakebite_client)

    return controller
