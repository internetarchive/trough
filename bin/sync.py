#!/usr/bin/env python3
import logging
import consulate
import settings
from snakebite.client import Client
import socket
import json
import os
import time
import random

class Segment(object):
    def __init__(self, consul, segment_id, size):
        self.consul = consul
        self.id = segment_id
        self.size = size
    def all_copies(self, full_record=False):
        ''' returns the 'assigned' SegmentCopies, whether or not they are 'up' '''
        assignments = []
        for host in get_hosts(type='read'):
            # then for each host, we'll check the k/v store
            record = None
            if full_record:
                record = consul.kv.get_record(host_database_key)
            elif consul.kv.get(self.host_key(host['hostname'])):
                record = host_database_key
                assignments.push(self.host_key(host['hostname']))
            if record:
                assignments.push(record)
        return assignments
    def up_copies(self):
        '''returns the 'up' SegmentCopies'''
        return consul.catalog.service("trough/read/%s" % database_name)
    def host_key(self, host):
        return "%s/%s" % (host, self.id)
    def is_assigned_to_host(self, host):
        return bool(self.consul.kv.get(self.host_key(host)))
    def minimum_assignments(self):
        '''This function should return the minimum number of assignments which is acceptable for a given database.'''
        return 2
        raise Exception('Not Implemented')

class HostRegistry(object):
    ''' this should probably implement all of the 'host' oriented functions below. '''
    def __init__(self, consul):
        self.consul = consul
    def get_hosts(self, type='read'):
        return self.consul.catalog.service('trough/%s' % type)
    def look_for_hosts(self):
        return bool(self.get_hosts(type='read') + self.get_hosts(type='write'))
    def host_load(self):
        output = []
        for host in self.get_hosts(type='read'):
            assigned_bytes = sum(self.consul.kv["%s/" % host['hostname'] ])
            total_bytes = self.consul.kv["%s" % host['hostname'] ]
            output.push({
                'hostname': host['hostname'],
                'remaining_bytes': total_bytes - assigned_bytes,
                'assigned_bytes': assigned_bytes,
                'total_bytes': total_bytes,
                'load_ratio': (total_bytes - assigned_bytes) / total_bytes
            })
        return output
    def host_bytes_remaining(self):
        output = self.host_load()
        return 
    def underloaded_hosts(self):
        output = []
        hosts = self.host_load()
        average_load_ratio = sum([host['load_ratio'] for host in hosts]) / len(hosts)
        # 5% below the average load is an acceptable ratio
        for host in hosts:
            # TODO: figure out a better way to describe the "acceptably empty" percentage.
            # Noah suggests (a multiplication factor) * (the largest database / total dataset size), capped at 1.0 (100%)
            if host['load_ratio'] < (average_load - 0.05):
                host['average_load_ratio'] = average_ratio
                output.push(host)
        return output
    def host_is_advertised(self, host):
        for host in self.get_hosts(type='read'):
            if host['hostname'] = host:
                return True
        return False
    def advertise(self, name, service_id, address=settings['EXTERNAL_IP'], \
            port=settings['READ_PORT'], tags=[], ttl=settings['READ_NODE_DNS_TTL']):
        self.consul.agent.service.register(name, service_id=service_id, address=address, port=port, tags=tags, ttl=ttl)
    def health_check(self, pool, service_name):
        return self.consul.health.node("trough/%s/%s" % (pool, service_name))
    def create_health_check(self, name, pool, service_name, ttl=ttl, notes=notes):
        return self.consul.agent.check.register(name, check_id="trough/%s/%s" % (pool, service_name), ttl=ttl, notes=notes)
    def reset_health_check(self, pool, service_name):
        return self.consul.agent.check.ttl_pass("trough/%s/%s" % (pool, service_name))
    def assign(self, host, segment):
        return self.consul.kv[segment.host_key(host)] = segment.size
    def unassign(self, host, segment):
        del consul.kv[segment.host_key(host)]
    def set_quota(self, host, quota):
        return self.consul.kv[host] = quota
    def segments_for_host(host):
        return [Segment(consul=self.consul, segment_id=k.split('/')[-1], size=v) for k, v in self.consul.kv.find("%s/" % host)]


##################################################
# SERVER/MASTER MODE
##################################################

def check_master_config():
    try:
        assert settings['HDFS_PATH'], "HDFS_PATH must be set, otherwise I don't know where to look for sqlite files."
        assert settings['HDFS_HOST'], "HDFS_HOST must be set, or I can't communicate with HDFS."
        assert settings['HDFS_PORT'], "HDFS_PORT must be set, or I can't communicate with HDFS."
        assert settings['ELECTION_CYCLE'] > 0, "ELECTION_CYCLE must be greater than zero. It governs the number of seconds in a sync master election period."
        assert settings['HOSTNAME'], "HOSTNAME must be set, or I can't figure out my own hostname."
        assert settings['EXTERNAL_IP'], "EXTERNAL_IP must be set. We need to know which IP to use."
        assert settings['SYNC_PORT'], "SYNC_PORT must be set. We need to know the output port."
    except AssertionError as e:
        sys.exit("{} Exiting...".format(str(e)))

def hold_election():
    logging.warn('Holding election')
    if consul.catalog.service('trough/sync/master'):
        output = consul.catalog.service('trough/sync/master')
        if output[0]['hostname'] == settings['HOSTNAME']:
            # 'touch' the ttl check for sync master
            logging.warn('Still the master. I will check again in %ss' % seconds=settings['ELECTION_CYCLE'])
            consul.agent.check.ttl_pass("trough/sync/master")
            return True
        logging.warn('I am not the master. I will check again in %ss' % seconds=settings['ELECTION_CYCLE'])
        return False
    else:
        logging.warn('There is no "trough/sync/master" service in consul. I am the master.')
        logging.warn('Setting up master service...')
        consul.agent.service.register('Trough Sync Master', 
                            service_id='trough/sync/master',
                            address=settings['EXTERNAL_IP'],
                            port=settings['SYNC_PORT'],
                            tags=['master'],
                            ttl=settings['ELECTION_CYCLE'] * 3)
        logging.warn('Setting up a health check, ttl %ss...' % settings['ELECTION_CYCLE'] * 3)
        consul.agent.check.register('Healthy Master Sync Node',
                            check_id="trough/sync/master",
                            ttl=settings['ELECTION_CYCLE'] * 3,
                            notes="Sync Servers hold an election every %ss. They are unhealthy after missing 2 elections" % settings['ELECTION_CYCLE'])
        return True

def run_sync_master():
    # TODO: this needs to execute periodically. how? cron? or long-running process?
    ''' 
    "server" mode:
    - if I am not the leader, poll forever
    - if there are hosts to assign to, poll forever.
    - for entire list of databases that match pattern in REMOTE_DATA setting:
        - check consul to make sure each item is assigned to a worker
        - if it is not assigned:
            - assign it, based on the available quota on each worker
        - if the number of assignments for this database are greater than they should be, and all copies are 'up':
            - unassign the copy with the lowest assignment index
    - for list of hosts:
        - if this host meets a "too empty" metric
            - loop over the databases
            - add extra assignments to the "too empty" host in a ratio of databases which corresponds to the delta from the average load.
    '''
    leader = False
    found_hosts = False
    consul = consulate.Consul()
    registry = HostRegistry(consul=consul)

    # hold an election every settings['ELECTION_CYCLE'] seconds
    while not leader:
        leader = hold_election(consul)
        sleep(settings['ELECTION_CYCLE'])
    while not found_hosts:
        found_hosts = look_for_hosts(consul)
        sleep(settings['HOST_CHECK_WAIT_PERIOD'])

    # we are assured that we are the master, and that we have machines to assign to.
    sb_client = Client(settings['HDFS_HOST'], settings['HDFS_PORT'])

    file_listing = sb_client.ls(settings['HDFS_PATH'])

    file_total = 0
    for file in file_listing:
        local_part = file['path'].split('/')[-1]
        local_part = local_part.replace('.sqlite', '')
        segment = Segment(consul=consul, segment_id=local_part, size=file['length'])
        if not segment.enough_copies():
            emptiest_host = sorted(registry.host_load(), key=lambda host: host['remaining_bytes'], reverse=True)[0]
            # assign the byte count of the file to a key named, e.g. /hostA/database
            registry.assign(emptiest_host, segment)
        else:
            # If we find too many 'up' copies
            if segment.up_copies() > segment.minimum_assignments():
                # If so, delete the copy with the lowest 'CreateIndex', which records the
                # order in which keys are created.
                assignments = segment.all_copies(full_record=True)
                assignments = sorted(assignments, key=lambda record: record['CreateIndex'])
                host = assignments[0].split("/")[0]
                # remove the assignment
                registry.unassign(host, segment)
        file_total += 1

    for host in registry.underloaded_hosts():
        '''while the load on this host is lower than the acceptable load, reassign 
        segments in the file listing order returned from snakebite.'''
        ratio_to_reassign = host['average_load_ratio'] - host['load_ratio']
        file_listing = sb_client.ls(settings['HDFS_PATH'])
        for file in file_listing:
            local_part = file['path'].split('/')[-1]
            local_part = local_part.replace('.sqlite', '')
            segment = Segment(consul=consul, segment_id=local_part, size=file['length'])
            # if this segment is already assigned to this host, next segment.
            if segment.is_assigned_to_host(host):
                continue
            # add an assignment for this segment to this host.
            registry.assign(host, segment)
            host['assigned_bytes'] += segment.size
            host['load_ratio'] = host['assigned_bytes'] * host['total_bytes']
            if host['load_ratio'] >= host['average_load_ratio'] * 0.95:
                break


##################################################
# LOCAL MODE
##################################################

def check_local_config():
    try:
        assert settings['HOSTNAME'], "HOSTNAME must be set, or I can't figure out my own hostname."
        assert settings['EXTERNAL_IP'], "EXTERNAL_IP must be set. We need to know which IP to use."
        assert settings['READ_PORT'], "SYNC_PORT must be set. We need to know the output port."
    except AssertionError as e:
        sys.exit("{} Exiting...".format(str(e)))

def check_database_exists(database):
    if os.file.exists(os.join(settings['LOCAL_DATA'], "%s.sqlite" % database)):
        return True
    return False

def check_database_matches_hdfs(sb_client, database):
    for listing in sb_client.ls(settings['HDFS_PATH']):
        if file['length'] == os.path.getsize(os.path.join(settings['LOCAL_DATA'], "%s.sqlite" % database)):
            return True
    return False

def copy_database_from_hdfs(database):
    source = [os.path.join(settings['HDFS_PATH'], "%s.sqlite" % database)]
    destination = settings['LOCAL_DATA']
    return sb_client.copyToLocal(source, destination)

def run_sync_local():
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
    consul = consulate.Consul()
    registry = HostRegistry(consul)

    my_hostname = settings['HOSTNAME']

    if not registry.host_is_advertised(my_hostname):
        registry.advertise('Trough Read Node', service_id='trough/nodes/%s' % my_hostname)

    # if there is a health check for this node
    if registry.health_check('nodes', my_hostname):
        # reset the countdown
        registry.reset_health_check('nodes', my_hostname)
    registry.set_quota(my_hostname, settings['STORAGE_IN_BYTES'])
    loop_timer = time.time()
    sb_client = Client(settings['HDFS_HOST'], settings['HDFS_PORT'])
    for segment in registry.segments_for_host(my_hostname):
        database_name = segment.id
        exists = check_database_exists(database_name)
        matches_hdfs = check_database_matches_hdfs(sb_client, database_name)
        if not exists or not matches_md5:
            copy_database_from_hdfs(sb_client, database_name)
            registry.advertise('Database %s' % database_name, service_id='trough/read/%s' % database_name, tags=[database_name])
            # to calculate the database TTL, use (settings['SYNC_LOOP_TIMING'] + total loop time, so far) * 2
            database_health_ttl = round(settings['SYNC_LOOP_TIMING'] + (loop_timer - time.time()) * 2)
            registry.create_health_check(name='Database %s Is Healthy' % database_name,
                            pool="read",
                            service_name=database_name,
                            ttl=database_health_ttl,
                            notes="Database Health Checks occur every %ss. They are unhealthy after missing (appx) 2 sync loops." % settings['SYNC_LOOP_TIMING'])
        registry.reset_health_check('read', database_name)
    if not registry.health_check('nodes', my_hostname):
        # to calculate the node TTL, use (settings['SYNC_LOOP_TIMING'] + total loop time) * 2
        node_health_ttl = round(settings['SYNC_LOOP_TIMING'] + (loop_timer - time.time()) * 2)
        registry.create_health_check(name='Node %s Is Healthy' % my_hostname,
                        pool="nodes",
                        service_name=my_hostname,
                        ttl=node_health_ttl,
                        notes="Node Health Checks occur every %ss. They are unhealthy after missing (appx) 2 sync loops." % settings['SYNC_LOOP_TIMING'])



# health check:
# - database health will be a TTL check, which means the databases are assumed to be 'unhealthy'
#   after some period of time.
# - query consul to see which databases are assigned to this host http://localhost:8500/v1/kv/{{ host }}/?keys
# - for each database:
#     - check the CRC/MD5 of the file against HDFS
#     - make a query to the file, on localhost HTTP by forcing a Host header. Maybe check that a specific
#       set of tables exists with SELECT name FROM sqlite_master WHERE type='table' AND name='{{ table_name }}';
# - ship a manifest upstream, or possibly for each item 
# for the node health check, receiving any healthy database message on the host will suffice.
# (aka the health check for DBs should also reset TTL for the host)
    
# health, assignments, and first "up" event:
# - assignments happen via pushing a key/value into the consul store ('/host/database_id': 'database_size') 
#   retrieve assignment list via: http://localhost:8500/v1/kv/host1/?keys
# - upon assignment, local synchronizer wakes up, copies file from hdfs.
# - upon copy completion, local synchronizer runs first health check.
# - upon first health check completion, synchronizer sets up DNS for databases on this host.
#   database is now 'up'
# - upon first health check completion, synchronizer sets up TTL-based future health checks
#   for each database assigned to this host.

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Run a "server" sync process, which controls other sync processes, ' \
        'or a "local" sync process, which loads segments onto the current machine and performs health checks.')

    parser.add_argument('--server', dest='server', action='store_true',
                        help='run in server or "master" mode, control the actions of other local synchronizers.')
    args = parser.parse_args()
    if args.server:
        check_master_config()
        run_sync_master()
    else:
        check_local_config()
        run_sync_local()