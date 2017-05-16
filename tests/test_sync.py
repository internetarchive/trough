import os
os.environ['TROUGH_LOG_LEVEL'] = 'ERROR'
os.environ['TROUGH_SETTINGS'] = os.path.join(os.path.dirname(__file__), "test.conf")

import unittest
from unittest import mock
from trough import sync
from trough.settings import settings
import sqlite3
from collections import defaultdict
import threading
import datetime
import time
import doublethink

class TestSegment(unittest.TestCase):
    def setUp(self):
        self.rethinker = doublethink.Rethinker(db="trough_configuration", servers=settings['RETHINKDB_HOSTS'])
        self.services = doublethink.ServiceRegistry(self.rethinker)
        self.registry = sync.HostRegistry(rethinker=self.rethinker, services=self.services)
        sync.ensure_tables(self.rethinker)
        self.rethinker.table("services").delete().run()
        self.rethinker.table("lock").delete().run()
        self.rethinker.table("assignment").delete().run()
    def test_host_key(self):
        segment = sync.Segment('test-segment',
            services=self.services,
            rethinker=self.rethinker,
            registry=self.registry,
            size=100)
        key = segment.host_key('test-node')
        self.assertEqual(key, 'test-node:test-segment')
    def test_all_copies(self):
        registry = sync.HostRegistry(rethinker=self.rethinker, services=self.services)
        segment = sync.Segment('test-segment',
            services=self.services,
            rethinker=self.rethinker,
            registry=self.registry,
            size=100)
        registry.assign(hostname='test-pool', segment=segment, remote_path="/fake/path")
        registry.commit_assignments()
        output = segment.all_copies()
        output = [item for item in output]
        self.assertEqual(output[0]['id'], 'test-pool:test-segment')
    def test_readable_copies(self):
        consul = mock.Mock()
        consul.catalog.service = mock.Mock(return_value=[{"node": "test-segment.test-node"}])
        registry = sync.HostRegistry(rethinker=self.rethinker, services=self.services)
        segment = sync.Segment('test-segment',
            services=self.services,
            rethinker=self.rethinker,
            registry=self.registry,
            size=100)
        registry.heartbeat(pool='trough-read',
            node=settings['HOSTNAME'],
            heartbeat_interval=0.1,
            segment=segment.id)
        output = segment.readable_copies()
        output = list(output)
        self.assertEqual(output[0]['node'], settings['HOSTNAME'])
    def test_is_assigned_to_host(self):
        segment = sync.Segment('test-segment',
            services=self.services,
            rethinker=self.rethinker,
            registry=self.registry,
            size=100)
        registry = sync.HostRegistry(rethinker=self.rethinker, services=self.services)
        registry.assign(hostname='assigned', segment=segment, remote_path="/fake/path")
        registry.commit_assignments()
        output = segment.is_assigned_to_host('not-assigned')
        self.assertFalse(output)
        output = segment.is_assigned_to_host('assigned')
        self.assertTrue(output)
    def test_minimum_assignments(self):
        segment = sync.Segment('123456',
            services=self.services,
            rethinker=self.rethinker,
            registry=self.registry,
            size=100)
        output = segment.minimum_assignments()
        self.assertEqual(output, 1)
        segment = sync.Segment('228188',
            services=self.services,
            rethinker=self.rethinker,
            registry=self.registry,
            size=100)
        output = segment.minimum_assignments()
        self.assertEqual(output, 2)
    def test_acquire_write_lock(self):
        lock = sync.Lock.load(self.rethinker, 'write:lock:123456')
        if lock:
            lock.release()
        segment = sync.Segment('123456',
            services=self.services,
            rethinker=self.rethinker,
            registry=self.registry,
            size=100)
        lock = segment.acquire_write_lock()
        with self.assertRaises(Exception):
            segment.acquire_write_lock()
        lock.release()
    def test_retrieve_write_lock(self):
        lock = sync.Lock.load(self.rethinker, 'write:lock:123456')
        if lock:
            lock.release()
        segment = sync.Segment('123456',
            services=self.services,
            rethinker=self.rethinker,
            registry=self.registry,
            size=100)
        output = segment.acquire_write_lock()
        lock = segment.retrieve_write_lock()
        self.assertEqual(lock["node"], settings['HOSTNAME'])
        self.assertIn("acquired_on", lock)
        lock.release()
    def test_local_path(self):
        segment = sync.Segment('123456',
            services=self.services,
            rethinker=self.rethinker,
            registry=self.registry,
            size=100)
        output = segment.local_path()
        self.assertEqual(output, os.path.join(settings['LOCAL_DATA'], '123456.sqlite'))
    def test_local_segment_exists(self):
        segment = sync.Segment('123456',
            services=self.services,
            rethinker=self.rethinker,
            registry=self.registry,
            size=100)
        output = segment.local_segment_exists()
        self.assertEqual(output, False)
    def test_provision_local_segment(self):
        segment = sync.Segment('123456-test-database',
            services=self.services,
            rethinker=self.rethinker,
            registry=self.registry,
            size=100)
        if segment.local_segment_exists():
            os.remove(segment.local_path())
        output = segment.provision_local_segment()
        connection = sqlite3.connect(segment.local_path())
        cursor = connection.cursor()
        results = cursor.execute('SELECT * FROM test;')
        output = [dict((cursor.description[i][0], value) for i, value in enumerate(row)) for row in results]
        connection.close()
        os.remove(segment.local_path())
        self.assertEqual(output, [{'id': 1, 'test': 'test'}])



class TestHostRegistry(unittest.TestCase):
    def setUp(self):
        self.rethinker = doublethink.Rethinker(db="trough_configuration", servers=settings['RETHINKDB_HOSTS'])
        self.services = doublethink.ServiceRegistry(self.rethinker)
        sync.ensure_tables(self.rethinker)
        self.rethinker.table("services").delete().run()
        self.rethinker.table("lock").delete().run()
        self.rethinker.table("assignment").delete().run()
    def test_get_hosts(self):
        hostname = 'test.example.com'
        registry = sync.HostRegistry(rethinker=self.rethinker, services=self.services)
        registry.heartbeat(pool='trough-nodes', service_id='trough:nodes:%s' % hostname, node=hostname, heartbeat_interval=0.2)
        output = registry.get_hosts()
        self.assertEqual(output[0]['node'], "test.example.com")
    def test_hosts_exist(self):
        hostname = 'test.example.com'
        registry = sync.HostRegistry(rethinker=self.rethinker, services=self.services)
        self.assertEqual(registry.hosts_exist(), False)
        registry.heartbeat(pool='trough-nodes', service_id='trough:nodes:%s' % hostname, node=hostname, heartbeat_interval=0.2)
        self.assertEqual(registry.hosts_exist(), True)
    def test_host_load(self):
        registry = sync.HostRegistry(rethinker=self.rethinker, services=self.services)
        hostname = 'test.example.com'
        registry.heartbeat(pool='trough-nodes',
            service_id='trough:nodes:%s' % hostname,
            node=hostname,
            heartbeat_interval=0.2,
            available_bytes=1024*1024)
        segment = sync.Segment('test-segment-1',
            services=self.services,
            rethinker=self.rethinker,
            registry=registry,
            size=1024)
        registry.assign(hostname='test.example.com', segment=segment, remote_path="/fake/path1")
        segment = sync.Segment('test-segment-2',
            services=self.services,
            rethinker=self.rethinker,
            registry=registry,
            size=1024)
        registry.assign(hostname='test.example.com', segment=segment, remote_path="/fake/path2")
        registry.commit_assignments()
        output = registry.host_load()
        self.assertEqual(output[0]['assigned_bytes'], 2048)
    def test_min_acceptable_load_ratio(self):
        ''' the min acceptable load ratio is a metric that represents the load ratio that 
        should be considered underloaded enough to reassign segments. A ratio of
        0 means that no segments will be reassigned. A ratio of 1 means that all segments
        will be reassigned. Numbers in between mean that segments will be reassigned from the
        highest-loaded to the lowest-loaded host until the target is achieved.'''
        # TODO: re-implement this test, after fixing the above.
        return None
        self.consul.kv['127.0.0.1'] = 1024 * 1024
        self.consul.kv['127.0.0.1/seg1'] = 1024
        self.consul.kv['127.0.0.1/seg2'] = 1024
        self.consul.kv['127.0.0.2'] = 1024 * 1024
        self.consul.kv['127.0.0.2/seg3'] = 1024
        self.consul.kv['127.0.0.2/seg4'] = 1024 * 1000
        services = defaultdict(list)
        def register(name, service_id="", address=None, port=0, tags=[], ttl=None):
            services[name].append({ "node": address })
        self.consul.agent.service.register = register
        def service(name):
            return services[name]
        self.consul.catalog.service = service
        registry = sync.HostRegistry(self.consul)
        hostname = 'test.example.com'
        registry.register('trough-nodes', service_id='trough/nodes/%s' % hostname, tags=[hostname], address='127.0.0.1')
        hostname = 'test2.example.com'
        registry.register('trough-nodes', service_id='trough/nodes/%s' % hostname, tags=[hostname], address='127.0.0.2')
        # zero segments
        output = registry.min_acceptable_load_ratio(registry.host_load(), 0)
        self.assertEqual(output, 0)
        # one segment
        output = registry.min_acceptable_load_ratio(registry.host_load(), 1024 * 1000)
        self.assertEqual(output, 0)
        # a few segments, exactly the same size, clear ratio to load
        self.consul.kv['127.0.0.1/seg1'] = 1024 * 128
        self.consul.kv['127.0.0.1/seg2'] = 1024 * 128
        self.consul.kv['127.0.0.1/seg5'] = 1024 * 128
        self.consul.kv['127.0.0.1/seg6'] = 1024 * 128
        self.consul.kv['127.0.0.2/seg3'] = 1024 * 128
        self.consul.kv['127.0.0.2/seg4'] = 1024 * 128
        self.consul.kv['127.0.0.2/seg7'] = 1024 * 128
        self.consul.kv['127.0.0.2/seg8'] = 1024 * 128
        output = registry.min_acceptable_load_ratio(registry.host_load(), 1024 * 128)
        self.assertEqual(output, 0.375)
        # a few segments, sized reasonably in re: the load
        for key in [key for key in self.consul.kv]:
            if "/" in key:
                del self.consul.kv[key]
        self.consul.kv['127.0.0.1/seg1'] = 1024 * 200
        self.consul.kv['127.0.0.1/seg2'] = 1024 * 200
        self.consul.kv['127.0.0.1/seg5'] = 1024 * 200
        self.consul.kv['127.0.0.1/seg6'] = 1024 * 200
        self.consul.kv['127.0.0.1/seg7'] = 1024 * 200
        self.consul.kv['127.0.0.2/seg3'] = 1024 * 200
        self.consul.kv['127.0.0.2/seg4'] = 1024 * 400
        self.consul.kv['127.0.0.2/seg8'] = 1024 * 200
        self.consul.kv['127.0.0.2/seg9'] = 1024 * 200
        output = registry.min_acceptable_load_ratio(registry.host_load(), 1024 * 400)
        self.assertTrue(output > 0)
    def test_heartbeat(self):
        '''This function unusually produces indeterminate output.'''
        hostname = 'test.example.com'
        registry = sync.HostRegistry(rethinker=self.rethinker, services=self.services)
        registry.heartbeat(pool='trough-nodes',
            service_id='trough:nodes:%s' % hostname,
            node=hostname,
            heartbeat_interval=0.1,
            available_bytes=1024*1024)
        hosts = registry.get_hosts()
        self.assertEqual(hosts[0]["node"], hostname)
        time.sleep(0.4)
        hosts = registry.get_hosts()
        self.assertEqual(hosts, [])
    def test_assign(self):
        registry = sync.HostRegistry(rethinker=self.rethinker, services=self.services)
        segment = sync.Segment('123456',
            services=self.services,
            rethinker=self.rethinker,
            registry=registry,
            size=1024)
        registry.assign('localhost', segment, '/fake/path')
        self.assertEqual(registry.assignment_queue._queue[0]['id'], 'localhost:123456')
        return (segment, registry)
    def test_commit_assignments(self):
        segment, registry = self.test_assign()
        registry.commit_assignments()
        output = [seg for seg in segment.all_copies()]
        self.assertEqual(output[0]['id'], 'localhost:123456')
    def test_segments_for_host(self):
        registry = sync.HostRegistry(rethinker=self.rethinker, services=self.services)
        segment = sync.Segment('123456',
            services=self.services,
            rethinker=self.rethinker,
            registry=registry,
            size=1024)
        asmt = registry.assign('localhost', segment, '/fake/path')
        registry.commit_assignments()
        output = registry.segments_for_host('localhost')
        self.assertEqual(output[0].id, '123456')
        asmt.unassign()
        output = registry.segments_for_host('localhost')
        self.assertEqual(output, [])

class TestMasterSyncController(unittest.TestCase):
    def setUp(self):
        self.rethinker = doublethink.Rethinker(db="trough_configuration", servers=settings['RETHINKDB_HOSTS'])
        self.services = doublethink.ServiceRegistry(self.rethinker)
        self.registry = sync.HostRegistry(rethinker=self.rethinker, services=self.services)
        self.snakebite_client = mock.Mock()
        self.rethinker.table("services").delete().run()
    def get_foreign_controller(self):
        controller = sync.MasterSyncController(rethinker=self.rethinker,
            services=self.services,
            registry=self.registry,
            snakebite_client=self.snakebite_client)
        controller.hostname = 'read02'
        controller.election_cycle = 0.1
        controller.sync_loop_timing = 0.01
        return controller
    def get_local_controller(self):
        # TODO: tight timings here point to the fact that 'doublethink.service.unique_service' should
        # be altered to return the object that is returned from the delta rather than doing a
        # separate query. setting the sync loop timing to the same as the election cycle will 
        # yield a situation in which the first query may succeed, and not update the row, but
        # the second query will not find it when it runs because it's passed its TTL.
        controller = sync.MasterSyncController(rethinker=self.rethinker,
            services=self.services,
            registry=self.registry,
            snakebite_client=self.snakebite_client)
        controller.election_cycle = 0.1
        controller.sync_loop_timing = 0.01
        return controller
    def test_hold_election(self):
        foreign_controller = self.get_foreign_controller()
        controller = self.get_local_controller()
        output = controller.hold_election()
        self.assertEqual(output, True)
        output = foreign_controller.hold_election()
        self.assertEqual(output, False)
        time.sleep(0.4)
        output = controller.hold_election()
        self.assertEqual(output, True)
        output = controller.hold_election()
        self.assertEqual(output, True)
    def test_wait_to_become_leader(self):
        # TODO: this test should be considered 'failing'. see above.
        time.sleep(0.1)
        foreign_controller = self.get_foreign_controller()
        foreign_controller.hold_election()
        controller = self.get_local_controller()
        begin = datetime.datetime.now()
        controller.wait_to_become_leader()
        end = datetime.datetime.now()
        runtime = end - begin
        self.assertTrue(0 < runtime.total_seconds() <= 1)
    def test_wait_for_hosts(self):
        THREAD_DELAY = 0.1
        THREAD_JITTER = 0.05
        hostname = 'test.example.com'
        controller = self.get_local_controller()
        def register_host():
            time.sleep(THREAD_DELAY)
            self.registry.heartbeat(pool='trough-nodes',
                service_id='trough:nodes:%s' % hostname,
                node=hostname,
                heartbeat_interval=0.1,
                available_bytes=1024*1024)
        t = threading.Thread(target=register_host)
        t.start()
        begin = datetime.datetime.now()
        controller.wait_for_hosts()
        end = datetime.datetime.now()
        runtime = end - begin
        mintime = THREAD_DELAY - (settings['ELECTION_CYCLE'] + THREAD_JITTER)
        maxtime = THREAD_DELAY + (settings['ELECTION_CYCLE'] + THREAD_JITTER)
        self.assertTrue(mintime <= runtime.total_seconds() <= maxtime)
    def test_get_segment_file_list(self):
        def ls(*args, **kwargs):
            yield {'length': 1024 * 1000, 'path': '/seg1.sqlite'}
        self.snakebite_client.ls = ls
        controller = sync.MasterSyncController(
            rethinker=self.rethinker,
            services=self.services,
            registry=self.registry,
            snakebite_client=self.snakebite_client)
        listing = controller.get_segment_file_list()
        for item in listing:
            self.assertEqual(item['length'], 1024*1000)
    def test_assign_segments(self):
        def ls(*args, **kwargs):
            yield {'length': 1024 * 1000, 'path': '/seg1.sqlite'}
        self.snakebite_client.ls = ls
        hostname = 'test.example.com'
        self.registry.heartbeat(pool='trough-nodes',
            service_id='trough:nodes:%s' % hostname,
            node=hostname,
            heartbeat_interval=0.1,
            available_bytes=1024*1024)
        controller = self.get_local_controller()
        controller.assign_segments()
        assignments = [asmt for asmt in self.rethinker.table('assignment').run()]
        self.assertEqual(len(assignments), 1)
        self.assertEqual(assignments[0]['bytes'], 1024 * 1000)
    def test_rebalance_hosts(self):
        # TODO: re-implement this test, after fixing the above.
        return None
        self.consul.kv['127.0.0.1'] = 1024 * 1024
        self.consul.kv['127.0.0.1/seg1'] = 1024
        self.consul.kv['127.0.0.1/seg2'] = 1024
        self.consul.kv['127.0.0.2'] = 1024 * 1024
        self.consul.kv['127.0.0.2/seg3'] = 1024
        self.consul.kv['127.0.0.2/seg4'] = 1024 * 1000
        self.consul.kv['127.0.0.3'] = 1024 * 1024
        services = defaultdict(list)
        def register(name, service_id="", address=None, port=0, tags=[], ttl=None):
            services[name].append({ "node": address })
        self.consul.agent.service.register = register
        def service(name):
            return services[name]
        self.consul.catalog.service = service
        hostname = 'test.example.com'
        self.registry.register('trough-nodes', service_id='trough/nodes/%s' % hostname, tags=[hostname], address='127.0.0.1')
        hostname = 'test2.example.com'
        self.registry.register('trough-nodes', service_id='trough/nodes/%s' % hostname, tags=[hostname], address='127.0.0.2')
        hostname = 'test3.example.com'
        self.registry.register('trough-nodes', service_id='trough/nodes/%s' % hostname, tags=[hostname], address='127.0.0.3')
        record_list = []
        def ls(*args, **kwargs):
            for record in record_list:
                yield record
        self.snakebite_client.ls = ls
        # zero segments
        controller = sync.MasterSyncController(
            rethinker=self.rethinker,
            services=self.services,
            registry=self.registry,
            snakebite_client=self.snakebite_client)
        controller.rebalance_hosts()
        self.assertEqual(len(self.consul.kv.find('127.0.0.3')), 1)
        # equal load
        record_list = [
            {'length': 1024 * 128, 'path': '/seg1.sqlite'},
            {'length': 1024 * 128, 'path': '/seg2.sqlite'},
            {'length': 1024 * 128, 'path': '/seg3.sqlite'},
            {'length': 1024 * 128, 'path': '/seg4.sqlite'},
            {'length': 1024 * 128, 'path': '/seg5.sqlite'},
            {'length': 1024 * 128, 'path': '/seg6.sqlite'},
            {'length': 1024 * 128, 'path': '/seg7.sqlite'},
            {'length': 1024 * 128, 'path': '/seg8.sqlite'},
        ]
        for key in [key for key in self.consul.kv]:
            if "/" in key:
                del self.consul.kv[key]
        self.consul.kv['127.0.0.1/seg1'] = 1024 * 128
        self.consul.kv['127.0.0.1/seg2'] = 1024 * 128
        self.consul.kv['127.0.0.1/seg5'] = 1024 * 128
        self.consul.kv['127.0.0.1/seg6'] = 1024 * 128
        self.consul.kv['127.0.0.1/seg9'] = 1024 * 128
        self.consul.kv['127.0.0.2/seg3'] = 1024 * 128
        self.consul.kv['127.0.0.2/seg4'] = 1024 * 128
        self.consul.kv['127.0.0.2/seg7'] = 1024 * 128
        self.consul.kv['127.0.0.2/seg8'] = 1024 * 128
        self.consul.kv['127.0.0.2/seg0'] = 1024 * 128
        host_load = self.registry.host_load()
        controller = sync.MasterSyncController(
            rethinker=self.rethinker,
            services=self.services,
            registry=self.registry,
            snakebite_client=self.snakebite_client)
        controller.rebalance_hosts()
        self.assertEqual(len(self.consul.kv.find('127.0.0.3')), 4)
        # one segment much larger than others
        record_list = [
            {'length': 1024 * 1000, 'path': '/seg1.sqlite'},
            {'length': 1024 * 128, 'path': '/seg2.sqlite'},
            {'length': 1024 * 128, 'path': '/seg3.sqlite'},
            {'length': 1024 * 128, 'path': '/seg4.sqlite'},
            {'length': 1024 * 128, 'path': '/seg5.sqlite'},
            {'length': 1024 * 128, 'path': '/seg6.sqlite'},
        ]
        for key in [key for key in self.consul.kv]:
            if "/" in key:
                del self.consul.kv[key]
        self.consul.kv['127.0.0.1/seg1'] = 1024 * 1000
        self.consul.kv['127.0.0.2/seg2'] = 1024 * 128
        self.consul.kv['127.0.0.2/seg3'] = 1024 * 128
        self.consul.kv['127.0.0.2/seg4'] = 1024 * 128
        self.consul.kv['127.0.0.2/seg5'] = 1024 * 128
        self.consul.kv['127.0.0.2/seg6'] = 1024 * 128
        host_load = self.registry.host_load()
        controller = sync.MasterSyncController(
            rethinker=self.rethinker,
            services=self.services,
            registry=self.registry,
            snakebite_client=self.snakebite_client)
        controller.rebalance_hosts()
        self.assertEqual(len(self.consul.kv.find('127.0.0.3')), 1)

    def test_sync(self):
        pass

class TestLocalSyncController(unittest.TestCase):
    def setUp(self):
        self.rethinker = doublethink.Rethinker(db="trough_configuration", servers=settings['RETHINKDB_HOSTS'])
        self.services = doublethink.ServiceRegistry(self.rethinker)
        self.registry = sync.HostRegistry(rethinker=self.rethinker, services=self.services)
        self.snakebite_client = mock.Mock()
        self.rethinker.table("services").delete().run()
    def get_controller(self):
        return sync.LocalSyncController(rethinker=self.rethinker,
            services=self.services,
            registry=self.registry,
            snakebite_client=self.snakebite_client)
    @mock.patch("trough.sync.os.path")
    def test_check_segment_matches_hdfs(self, path):
        test_value = 100
        def gs(*args, **kwargs):
            return test_value
        path.getsize = gs
        record_list = [{'length': 100, 'path': '/test-segment.sqlite'}]
        def ls(*args, **kwargs):
            for record in record_list:
                yield record
        self.snakebite_client.ls = ls
        controller = self.get_controller()
        segment = sync.Segment('test-segment',
            services=self.services,
            rethinker=self.rethinker,
            registry=self.registry,
            size=100)
        output = controller.check_segment_matches_hdfs(segment)
        self.assertEqual(output, True)
        test_value = 50
        output = controller.check_segment_matches_hdfs(segment)
        self.assertEqual(output, False)
    # v don't log out the error message on error test below.
    @mock.patch("trough.sync.logging.error")
    def test_copy_segment_from_hdfs(self, error):
        results = [{'error': 'test error'}]
        def copyToLocal(*args, **kwargs):
            for result in results:
                yield result
        self.snakebite_client.copyToLocal = copyToLocal
        controller = self.get_controller()
        segment = sync.Segment('test-segment',
            services=self.services,
            rethinker=self.rethinker,
            registry=self.registry,
            size=100)
        with self.assertRaises(Exception):
            output = controller.copy_segment_from_hdfs(segment)
        results = [{}]
        output = controller.copy_segment_from_hdfs(segment)
        self.assertEqual(output, True)
    def test_heartbeat(self):
        controller = self.get_controller()
        controller.heartbeat()
        output = [svc for svc in self.rethinker.table('services').run()]
        self.assertEqual(output[0]['node'], 'read01')
        self.assertEqual(output[0]['first_heartbeat'], output[0]['last_heartbeat'])
    @mock.patch("trough.sync.logging.error")
    def test_sync_segments(self, error):
        segments = [sync.Segment('test-segment', services=self.services, rethinker=self.rethinker, registry=self.registry, size=100)]
        def segments_for_host(*args, **kwargs):
            return segments
        self.registry.segments_for_host = segments_for_host
        record_list = [{'length': 100, 'path': '/test-segment.sqlite'}]
        def ls(*args, **kwargs):
            for record in record_list:
                yield record
        self.snakebite_client.ls = ls
        results = [{'error': 'test error'}]
        def copyToLocal(*args, **kwargs):
            for result in results:
                yield result
        self.snakebite_client.copyToLocal = copyToLocal
        called = []
        def check_call(*args, **kwargs):
            called.append(True)
        self.registry.heartbeat = check_call
        controller = self.get_controller()
        with self.assertRaises(Exception):
            controller.sync_segments()
        results = [{}]
        controller.sync_segments()
        self.assertEqual(called, [True])
    def test_provision_writable_segment(self):
        test_segment = sync.Segment('test',
            services=self.services,
            rethinker=self.rethinker,
            registry=self.registry,
            size=0)
        test_path = test_segment.local_path()
        if os.path.isfile(test_path):
            os.remove(test_path)
        called = []
        controller = self.get_controller()
        controller.provision_writable_segment('test')
        self.assertEqual(os.path.isfile(test_path), True)
        os.remove(test_path)

if __name__ == '__main__':
    unittest.main()
