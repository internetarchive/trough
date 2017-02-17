#!/usr/bin/env python3
import logging
import consulate
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

class Segment(object):
    def __init__(self, consul, segment_id, size, registry):
        self.consul = consul
        self.id = segment_id
        self.size = int(size)
        self.registry = registry
    def host_key(self, host):
        return "%s/%s" % (host, self.id)
    def all_copies(self, full_record=False):
        ''' returns the 'assigned' segment copies, whether or not they are 'up' '''
        assignments = []
        for host in self.registry.get_hosts():
            # then for each host, we'll check the k/v store
            record = None
            if full_record:
                record = self.consul.kv.get_record(self.host_key(host['Node']))
            elif self.consul.kv.get(self.host_key(host['Node'])):
                record = self.host_key(host['Node'])
            if record:
                assignments.append(record)
        return assignments
    def readable_copies(self):
        '''returns the 'up' copies of this segment to read from, per consul.'''
        return self.consul.catalog.service("trough/read/%s" % self.id)
    def writable_copies(self):
        '''returns the 'up' copies of this segment to write to, per consul.'''
        return self.consul.catalog.service("trough/write/%s" % self.id)
    def is_assigned_to_host(self, host):
        return bool(self.consul.kv.get(self.host_key(host)))
    def minimum_assignments(self):
        '''This function should return the minimum number of assignments which is acceptable for a given segment.'''
        if hasattr(settings['MINIMUM_ASSIGNMENTS'], "__call__"):
            return settings['MINIMUM_ASSIGNMENTS'](self.id)
        else:
            return settings['MINIMUM_ASSIGNMENTS']
    def acquire_write_lock(self, host):
        '''Raises exception if required parameter "host" is not provided. Raises exception if lock exists.'''
        lock_key = 'write/locks/%s' % self.id
        if self.consul.kv.get(lock_key):
            raise Exception('Segment is already locked.')
        lock_data = {'Node': host, 'Datestamp': datetime.datetime.now().isoformat() }
        self.consul.kv[lock_key] = json.dumps(lock_data)
        return lock_data
    def retrieve_write_lock(self):
        '''Returns None or dict. Can be used to evaluate whether a lock exists and, if so, which host holds it.'''
        return json.loads(self.consul.kv.get('write/locks/%s' % self.id, "null"))
    def release_write_lock(self):
        '''Delete a lock. Raises exception in the case that the lock does not exist.'''
        del self.consul.kv['write/locks/%s' % self.id]
    def local_path(self):
        return os.path.join(settings['LOCAL_DATA'], "%s.sqlite" % self.id)
    def remote_path(self):
        return os.path.join(settings['HDFS_PATH'], "%s.sqlite" % self.id)
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
    ''' this should probably implement all of the 'host' oriented functions below. '''
    def __init__(self, consul):
        self.consul = consul
    def get_hosts(self):
        return self.consul.catalog.service('trough-nodes')
    def hosts_exist(self):
        output = bool(self.get_hosts())
        logging.debug("Looking for hosts. Found: %s" % output)
        return output
    def host_load(self):
        output = []
        for host in self.get_hosts():
            assigned_bytes = sum([int(value) for value in self.consul.kv.find('%s/' % host['Node']).values()])
            total_bytes = self.consul.kv.get("%s" % host['Node'])
            total_bytes = 0 if total_bytes in ['null', None] else int(total_bytes)
            output.append({
                'Node': host['Node'],
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
    def host_is_registered(self, host):
        logging.info('Checking if "%s" is registered.' % host)
        for registered_host in self.get_hosts():
            if registered_host['Node'] == host:
                logging.info('Found that "%s" is registered.' % host)
                return True
        return False
    def register(self, name, service_id, address=settings['EXTERNAL_IP'], \
            port=settings['READ_PORT'], tags=[], ttl=str(settings['READ_NODE_DNS_TTL']) + 's'):
        if type(ttl) == int:
            ttl = "%ss" % ttl
        logging.info('Registering: name[%s] service_id[%s] at /v1/catalog/service/%s on IP %s:%s with TTL %s' % (name, service_id, service_id, address, port, ttl))
        self.consul.agent.service.register(name, service_id=service_id, address=address, port=port, tags=tags, ttl=ttl)
    def deregister(self, name, service_id):
        logging.info('Deregistering: name[%s] service_id[%s] at /v1/catalog/service/%s' % (name, service_id, service_id))
        self.consul.agent.service.deregister(service_id=service_id)
    def reset_health_check(self, pool, service_name):
        logging.info('Updating health check for pool: "%s", service_name: "%s".' % (pool, service_name))
        return self.consul.agent.check.ttl_pass("service:trough/%s/%s" % (pool, service_name))
    def assign(self, hostname, segment):
        logging.info("Assigning segment: %s to '%s'" % (segment.id, hostname))
        logging.info('Setting key "%s" to "%s"' % (segment.host_key(hostname), segment.size))
        self.consul.kv[segment.host_key(hostname)] = segment.size
    def unassign(self, hostname, segment):
        logging.info("Unassigning segment: %s on '%s'" % (segment, hostname))
        del self.consul.kv[segment.host_key(hostname)]
    def set_quota(self, host, quota):
        logging.info('Setting quota for host "%s": %s bytes.' % (host, quota))
        self.consul.kv[host] = quota
    def segments_for_host(self, host):
        segments = [Segment(consul=self.consul, segment_id=k.split('/')[-1], size=v, registry=self) for k, v in self.consul.kv.find("%s/" % host).items()]
        logging.info('Checked for segments assigned to %s: Found %s segment(s)' % (host, len(segments)))
        return segments

# Base class, not intended for use.
class SyncController:
    def __init__(self, consul=None, registry=None, snakebite_client=None):
        self.consul = consul
        self.registry = registry
        self.snakebite_client = snakebite_client
        self.leader = False
        self.found_hosts = False
    def check_config(self):
        raise Exception('Not Implemented')
    def check_consul_health(self):
        try:
            random.seed(settings['HOSTNAME'])
            random_key = ''.join(random.choice(string.ascii_uppercase + string.digits) for i in range(10))
            logging.error("Inserting random key '%s' into consul's key/value store as a health check." % (random_key,))
            self.consul.kv[random_key] = True
            del self.consul.kv[random_key]
        except Exception as e:
            sys.exit('Unable to connect to consul. Exiting to prevent running in a bad state.')
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
            assert settings['CONSUL_ADDRESS'], "CONSUL_ADDRESS must be set. Where can I contact consul's RPC interface?"
            assert settings['CONSUL_PORT'], "CONSUL_PORT must be set. Where can I contact consul's RPC interface?"
        except AssertionError as e:
            sys.exit("{} Exiting...".format(str(e)))

    def hold_election(self):
        logging.info('Holding Sync Master Election...')
        sync_master_hosts = self.registry.consul.catalog.service('trough-sync-master')
        if sync_master_hosts:
            if sync_master_hosts[0]['Node'] == settings['HOSTNAME']:
                # 'touch' the ttl check for sync master
                logging.info('Still the master. I will check again in %ss' % settings['ELECTION_CYCLE'])
                self.registry.reset_health_check(pool='sync', service_name='master')
                return True
            logging.info('I am not the master. I will check again in %ss' % settings['ELECTION_CYCLE'])
            return False
        else:
            logging.info('There is no "trough-sync-master" service in consul. I am the master.')
            logging.info('Setting up master service...')
            self.registry.register('trough-sync-master',
                service_id='trough/sync/master',
                port=settings['SYNC_PORT'],
                tags=['master'],
                ttl=settings['ELECTION_CYCLE'] + settings['SYNC_LOOP_TIMING'] * 2)
            self.registry.reset_health_check(pool='sync', service_name='master')
            return True

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
            segment = Segment(consul=self.consul, segment_id=local_part, size=file['length'], registry=self.registry)
            if not len(segment.all_copies()) >= segment.minimum_assignments():
                emptiest_host = sorted(self.registry.host_load(), key=lambda host: host['assigned_bytes'])[0]
                # assign the byte count of the file to a key named, e.g. /hostA/segment
                self.registry.assign(emptiest_host['Node'], segment)
            else:
                # If we find too many 'up' copies
                if len(segment.readable_copies()) > segment.minimum_assignments():
                    # delete the copy with the lowest 'CreateIndex', which records the
                    # order in which keys are created.
                    assignments = segment.all_copies(full_record=True)
                    assignments = sorted(assignments, key=lambda record: record['CreateIndex'])
                    host = assignments[0].split("/")[0]
                    # remove the assignment
                    self.registry.unassign(host, segment)
            writable_copies = segment.writable_copies()
            if writable_copies:
                logging.info("Segment %s has a writable copy. It will be decommissioned in favor of the read-only copy from HDFS." % segment.id)
                self.decommission_writable_segment(segment)

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
        size_lookup = {}
        last_reassignment = None
        while sorted_hosts[-1]['load_ratio'] < min_acceptable_load and len(sorted_hosts) >= 2:
            # get the top-loaded host
            top_host = sorted_hosts[0]
            underloaded_host = sorted_hosts[-1]
            # if we haven't seen this host before, cache a list of its segments
            if top_host['Node'] not in assignment_cache:
                assignment_cache[top_host['Node']] = self.consul.kv.find("%s/" % top_host['Node'], separator='-')
                # shuffle segment order so when we .pop() it selects a random segment.
                random.shuffle(assignment_cache[top_host['Node']])
            # pick a segment assigned to the top-loaded host
            reassign_from = assignment_cache[top_host['Node']].pop()
            segment_name = reassign_from.split("/")[1]
            reassign_to = "%s/%s" % (underloaded_host['Node'], segment_name)
            reassignment = (reassign_from, reassign_to)
            segment_bytes = self.consul.kv[reassign_from]
            # if our last reassignment is the same as the current reassignment, there's only one segment on this host. Pop it.
            if reassign_from == last_reassignment:
                hosts.pop(0)
                continue
            last_reassignment = reassign_from
            # if this segment is already assigned to the bottom loaded host, put it back in at the top of the list and loop.
            if self.consul.kv.get(reassign_to):
                assignment_cache[top_host['Node']].insert(0, reassign_from)
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
            size_lookup[reassign_from] = segment_bytes
            sorted_hosts.sort(key=lambda host: host['load_ratio'], reverse=True)
        # perform the queued reassignments. We set a key value pair in consul to assign.
        for reassignment in queue:
            self.consul.kv[reassignment[1]] = size_lookup[reassignment[0]]

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
        # make sure consul is running. If not, die.
        self.check_consul_health()
        # loops indefinitely waiting to become leader.
        self.wait_to_become_leader()
        # loops indefinitely waiting for hosts to hosts to which to assign segments
        self.wait_for_hosts()
        # assign each segment we found in our HDFS file listing
        self.assign_segments()
        # rebalance the hosts in case any new servers join the cluster
        self.rebalance_hosts()

    def wait_for_write_lock(self, segment_id):
        while not lock:
            try:
                lock = self.consul.lock.acquire("master/%s" % segment_id)
            except consulate.exceptions.LockException as e:
                pass
        return lock

    def provision_writable_segment(self, segment_id):
        # to protect against querying the non-leader
        # get the hostname of the leader
        # if not my hostname, raise exception
        # acquire a lock for the process of provisioning
        # with lock:
        with self.wait_for_write_lock():
            segment = Segment(segment_id=segment_id, consul=self.consul, registry=self.registry)
            writable_copies = segment.writable_copies()
            readable_copies = segment.readable_copies()
            # if the requested segment has no writable copies:
            if len(writable_copies) == 0:
                all_hosts = registry.get_hosts()
                assigned_host = random.choice(readable_copies) if readable_copies else random_choice(all_hosts)
                # make request to node to complete the local sync
                post_url = 'http://%s:%s/' % (assigned_host, settings['SYNC_PORT'])
                requests.post(post_url, segment_id)
                # create a new consul service
                self.registry.register("trough-write-segments", service_id='trough/write/%s' % segment_id, tags=[segment_id])
            else:
                assigned_host = writable_copies[0]
        # return an http endpoint for POSTs
        return "http://%s.trough-write-segments.service.consul/" % (segment_id, )
        # implicitly release provisioning lock

    def decommission_writable_segment(self, segment):
        logging.info('De-commissioning a writable segment: %s' % segment.id)
        self.registry.deregister(name="trough-write-segments", service_id='trough/write/%s' % segment.id)



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
            assert settings['CONSUL_ADDRESS'], "CONSUL_ADDRESS must be set. Where can I contact consul's RPC interface?"
            assert settings['CONSUL_PORT'], "CONSUL_PORT must be set. Where can I contact consul's RPC interface?"
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

    def ensure_registered(self):
        if not self.registry.host_is_registered(self.hostname):
            logging.warning('I am not registered. Registering myself as "%s".' % self.hostname)
            self.registry.register('trough-nodes', service_id='trough/nodes/%s' % self.hostname, tags=[self.hostname], ttl=round(settings['SYNC_LOOP_TIMING'] * 2))

    def reset_health_check(self):
        logging.warning('Updating health check for "%s".' % self.hostname)
        # reset the countdown
        self.registry.reset_health_check('nodes', self.hostname)

    def sync_segments(self):
        segment_health_ttl = settings['SYNC_LOOP_TIMING'] * 2
        for segment in self.registry.segments_for_host(self.hostname):
            exists = segment.local_segment_exists()
            matches_hdfs = self.check_segment_matches_hdfs(segment)
            if not exists or not matches_hdfs:
                self.copy_segment_from_hdfs(segment)
            logging.info('registering segment %s...' % (segment.id))
            self.registry.register('trough-read-segments', service_id='trough/read/%s' % segment.id, tags=[segment.id], ttl=segment_health_ttl)
            self.registry.reset_health_check('read', segment.id)

    def provision_writable_segment(self, segment_id):
        # instantiate the segment
        segment = Segment(segment_id=segment_id, consul=self.consul, registry=self.registry, size=None)
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
        # make sure consul is up and running.
        self.check_consul_health()
        # ensure that I am registered in consul, and set up a TTL health check.
        self.ensure_registered()
        # reset the TTL health check for this node. I am still alive!
        self.reset_health_check()
        # set my quota
        self.registry.set_quota(self.hostname, settings['STORAGE_IN_BYTES'])
        # sync my local segments with HDFS
        self.sync_segments()

def get_controller(server_mode):
    logging.info('Connecting to Consul for on: %s:%s' % (settings['CONSUL_ADDRESS'], settings['CONSUL_PORT']))
    consul = consulate.Consul(host=settings['CONSUL_ADDRESS'], port=settings['CONSUL_PORT'])
    registry = HostRegistry(consul=consul)
    logging.info('Connecting to HDFS on: %s:%s' % (settings['HDFS_HOST'], settings['HDFS_PORT']))
    snakebite_client = Client(settings['HDFS_HOST'], settings['HDFS_PORT'])

    if server_mode:
        controller = MasterSyncController(
            registry=registry,
            consul=consul,
            snakebite_client=snakebite_client)
    else:
        controller = LocalSyncController(
            registry=registry,
            consul=consul,
            snakebite_client=snakebite_client)

    return controller

# freestanding script entrypoint
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Run a "server" sync process, which controls other sync processes, ' \
        'or a "local" sync process, which loads segments onto the current machine and performs health checks.')

    parser.add_argument('--server', dest='server', action='store_true',
                        help='run in server or "master" mode: control the actions of other local synchronizers.')
    args = parser.parse_args()

    controller = get_controller(args.server)
    controller.check_config()
    while True:
        controller.sync()
        logging.info('Sleeping for %s seconds' % settings['SYNC_LOOP_TIMING'])
        time.sleep(settings['SYNC_LOOP_TIMING'])


# wsgi entrypoint
def application(env, start_response):
    # need a way to differentiate server mode from local mode. Store in `env`?
    try:
        controller = get_controller(env.get('master_mode', False))
        controller.check_config()
        segment_name = env.get('wsgi.input').read()
        output = controller.provision_writable_segment(segment_name)
        start_response()
        return output
    except Exception as e:
        start_response('500 Server Error', [('Content-Type', 'text/plain')])
        return [b'500 Server Error: %s' % str(e).encode('utf-8')]
