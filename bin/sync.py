#!/usr/bin/env python3
import logging
import consulate
import settings
from snakebite.client import Client
import socket
import json
import os
import time

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

def look_for_hosts():
    return bool(consul.catalog.service('trough/read') + consul.catalog.service('trough/write'))

def host_bytes_remaining(consul):
    output = []
    for host in consul.catalog.service('trough/read'):
        served_bytes = sum(consul.kv["%s/" % host['hostname'] ])
        total_bytes = consul.kv["%s" % host['hostname'] ]
        output.push({'host': host['hostname'], 'remaining_bytes': total_bytes - served_bytes})
    return sorted(output, key=lambda host: host['remaining_bytes'], reverse=True)

def is_assigned(consul, database_name):
    # check service registry to see if the database is 'up' (O(1))
    output = consul.catalog.service(database_name)
    if output:
        return True
    # if database is not up, check k/v store to see if has been assigned:
    # for each host, check if the database is in the k/v for that host.
    # this scales linearly with hosts, not databases. (O(n) where n = # of hosts)
    # this line looks up hosts that are available for read
    # note that any unhealthy nodes will be filtered out at this point.
    for host in consul.catalog.service('trough/read'):
        # then for each host, we'll check the k/v store
        try:
            if consul.kv["%s/%s" % (host.hostname, database_name)]:
                return True
        # if we didn't find the key, keep looking
        except KeyError as e:
            continue
    # the key is not present, and the service doesn't exist. It's not assigned.
    return False


def sync_master():
    # TODO: this needs to execute periodically. how? cron? or long-running process?
    ''' 
    "server" mode:
    - if I am not the leader, poll forever
    - if there are hosts to assign to, poll forever.
    - for entire list of databases that match pattern in REMOTE_DATA setting:
        - check consul to make sure each item is assigned to a worker
        - if it is not assigned:
            - assign it, based on the available quota on each worker
        TODO: - if the number of assignments for this database are greater than they should be, and all copies are 'up':
            TODO: - unassign the oldest copy (this requires a change to k/v store that has datetime)
    TODO: - for list of hosts:
        TODO: - if this host meets a "too empty" metric
            TODO: - loop over the databases
            TODO: - add an extra assignment to the new host for every Nth database, where n is the number of 'up' nodes
    '''
    leader = False
    found_hosts = False
    consul = consulate.Consul()

    # hold an election every settings['ELECTION_CYCLE'] seconds
    while not leader:
        leader = hold_election(consul)
        sleep(settings['ELECTION_CYCLE'])
    while not found_hosts:
        found_hosts = look_for_hosts(consul)
        sleep(settings['HOST_CHECK_WAIT_PERIOD'])

    # we are assured that we are the master, and that we have machines to assign to.
    sb_client = Client(settings['HDFS_HOST'], settings['HDFS_PORT'], use_trash=False)
    file_listing = sb_client.ls(settings['HDFS_PATH'])

    for file in file_listing:
        local_part = file['path'].split('/')[-1]
        local_part = local_part.replace('.sqlite', '')
        if not is_assigned(consul, local_part):
            emptiest_host = host_bytes_remaining(consul)[0]
            host_database_key = "%s/%s" % (emptiest_host, local_part)
            # assign the byte count of the file to a key named, e.g. /hostA/database
            consul.kv[host_database_key] = file['length']
        # we must deal with rebalancing: see TODOs above.
        host_bytes_remaining(consul)[0]

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

def have_advertised_myself(consul):
    for host in consul.catalog.service('trough/read'):
        if host['hostname'] = settings['HOSTNAME']:
            return True
    return False

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

def sync_local():
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

    if not have_advertised_myself(consul):
        consul.agent.service.register('Trough Read Node', 
                                service_id='trough/nodes/%s' % settings['HOSTNAME'],
                                address=settings['EXTERNAL_IP'],
                                port=settings['READ_PORT'],
                                tags=[],
                                ttl=settings['READ_NODE_DNS_TTL'])
    # if there is a health check for this node
    if consul.health.node("trough/nodes/%s" % settings['HOSTNAME']):
        # reset the countdown
        consul.agent.check.ttl_pass("trough/nodes/%s" % settings['HOSTNAME'])
    consul.kv[settings['HOSTNAME']] = settings['STORAGE_IN_BYTES']
    loop_timer = time.time()
    sb_client = Client(settings['HDFS_HOST'], settings['HDFS_PORT'], use_trash=False)
    for assignment in consul.kv.find(['%s/' % settings['HOSTNAME']]):
        database_name = assignment.split("/")[-1]
        exists = check_database_exists(database_name)
        matches_hdfs = check_database_matches_hdfs(sb_client, database_name)
        if not exists or not matches_md5:
            copy_database_from_hdfs(sb_client, database_name)
            consul.agent.service.register('Database %s' % database_name, 
                            service_id='trough/read/%s' % database_name,
                            address=settings['EXTERNAL_IP'],
                            port=settings['READ_PORT'],
                            tags=[database_name],
                            ttl=settings['READ_DATABASE_DNS_TTL'])
            # to calculate the database TTL, use (settings[''] + total loop time, so far) * 2
            database_health_ttl = round(settings['SYNC_LOOP_TIMING'] + (loop_timer - time.time()) * 2)
            consul.agent.check.register('Database %s Is Healthy' % database_name,
                            check_id="trough/read/%s" % database_name,
                            ttl=database_health_ttl,
                            notes="Database Health Checks occur every %ss. They are unhealthy after missing (appx) 2 sync loops." % settings['SYNC_LOOP_TIMING'])
        consul.agent.check.ttl_pass('trough/read/%s' % database_name)
    if not consul.health.node("trough/nodes/%s" % settings['HOSTNAME']):
        node_health_ttl = round(settings['SYNC_LOOP_TIMING'] + (loop_timer - time.time()) * 2)
        consul.agent.check.register('Node %s Is Healthy' % settings['HOSTNAME'],
                        check_id="trough/nodes/%s" % settings['HOSTNAME'],
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
        sync_server()
    else:
        check_local_config()
        sync_local()