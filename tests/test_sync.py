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

class TestSegment(unittest.TestCase):
    def setUp(self):
        self.consul = mock.Mock()
        self.consul.kv = {}
        self.registry = sync.HostRegistry(self.consul)
    def test_host_key(self):
        segment = sync.Segment(self.consul, 'test-segment', 100, self.registry)
        key = segment.host_key('test-node')
        self.assertEqual(key, 'test-node/test-segment')
    def test_all_copies(self):
        consul = mock.Mock()
        consul.kv.get = mock.Mock(return_value='100')
        consul.kv.get_record = mock.Mock(return_value={"Full": "Record"})
        self.consul.catalog.service = mock.Mock(return_value=[{"Node": "test-node"}])
        segment = sync.Segment(consul, 'test-segment', 100, self.registry)
        output = segment.all_copies()
        self.assertEqual(output, ['test-node/test-segment'])
        output = segment.all_copies(full_record=True)
        self.assertEqual(output, [{"Full": "Record"}])
    def test_readable_copies(self):
        consul = mock.Mock()
        consul.catalog.service = mock.Mock(return_value=[{"Node": "test-segment.test-node"}])
        segment = sync.Segment(consul, 'test-segment', 100, self.registry)
        output = segment.readable_copies()
        self.assertEqual(output, [{"Node": "test-segment.test-node"}])
    def test_is_assigned_to_host(self):
        consul = mock.Mock()
        consul.kv = { 'assigned/test-segment': 100 }
        segment = sync.Segment(consul, 'test-segment', 100, self.registry)
        output = segment.is_assigned_to_host('not-assigned')
        self.assertFalse(output)
        output = segment.is_assigned_to_host('assigned')
        self.assertTrue(output)
    def test_minimum_assignments(self):
        segment = sync.Segment(self.consul, '123456', 100, self.registry)
        output = segment.minimum_assignments()
        self.assertEqual(output, 1)
        segment = sync.Segment(self.consul, '228188', 100, self.registry)
        output = segment.minimum_assignments()
        self.assertEqual(output, 2)
    def test_acquire_write_lock(self):
        segment = sync.Segment(self.consul, '123456', 100, self.registry)
        segment.acquire_write_lock('node01')
        with self.assertRaises(Exception):
            segment.acquire_write_lock('node02')
    def test_retrieve_write_lock(self):
        segment = sync.Segment(self.consul, '123456', 100, self.registry)
        segment.acquire_write_lock('node01')
        output = segment.retrieve_write_lock()
        self.assertEqual(output["Node"], "node01")
        self.assertIn("Datestamp", output)
    def test_release_write_lock(self):
        segment = sync.Segment(self.consul, '123456', 100, self.registry)
        segment.acquire_write_lock('node01')
        segment.release_write_lock()
        output = segment.retrieve_write_lock()
        self.assertEqual(output, None)
    def test_local_path(self):
        segment = sync.Segment(self.consul, '123456', 100, self.registry)
        output = segment.local_path()
        self.assertEqual(output, os.path.join(settings['LOCAL_DATA'], '123456.sqlite'))
    def test_local_segment_exists(self):
        segment = sync.Segment(self.consul, '123456', 100, self.registry)
        output = segment.local_segment_exists()
        self.assertEqual(output, False)
    def test_provision_local_segment(self):
        segment = sync.Segment(self.consul, '123456-test-database', 100, self.registry)
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
        self.consul = mock.Mock()
        class D(dict):
            def find(self, arg, separator=None):
                output = {}
                for key in self.keys():
                    if key.startswith(arg):
                        output[key] = self[key]
                if separator:
                    return [val for val in output.values()]
                else:
                    return output
        self.consul.kv = D({})
    def test_get_hosts(self):
        services = defaultdict(list)
        def register(name, service_id="", address=None, port=0, tags=[], ttl=None):
            services[name] = [{ "Node": address }]
        self.consul.agent.service.register = register
        def service(name):
            return services[name]
        self.consul.catalog.service = service
        hostname = 'test.example.com'
        registry = sync.HostRegistry(self.consul)
        registry.register('trough-nodes', service_id='trough/nodes/%s' % hostname, tags=[hostname])
        output = registry.get_hosts()
        self.assertEqual(output, [{"Node": "127.0.0.1"}])
    def test_hosts_exist(self):
        services = defaultdict(list)
        def register(name, service_id="", address=None, port=0, tags=[], ttl=None):
            services[name] = [{ "Node": address }]
        self.consul.agent.service.register = register
        def service(name):
            return services[name]
        self.consul.catalog.service = service
        hostname = 'test.example.com'
        registry = sync.HostRegistry(self.consul)
        self.assertEqual(registry.hosts_exist(), False)
        registry.register('trough-nodes', service_id='trough/nodes/%s' % hostname, tags=[hostname])
        self.assertEqual(registry.hosts_exist(), True)
    def test_host_load(self):
        self.consul.kv['127.0.0.1'] = 1024 * 1024
        self.consul.kv['127.0.0.1/seg1'] = 1024
        self.consul.kv['127.0.0.1/seg2'] = 1024
        services = defaultdict(list)
        def register(name, service_id="", address=None, port=0, tags=[], ttl=None):
            services[name] = [{ "Node": address }]
        self.consul.agent.service.register = register
        def service(name):
            return services[name]
        self.consul.catalog.service = service
        hostname = 'test.example.com'
        registry = sync.HostRegistry(self.consul)
        registry.register('trough-nodes', service_id='trough/nodes/%s' % hostname, tags=[hostname])
        output = registry.host_load()
        self.assertEqual(output[0]['assigned_bytes'], 2048)
    def test_min_acceptable_load_ratio(self):
        ''' the min acceptable load ratio is a metric that represents a decision about what
        load ratio should be considered underloaded enough to reassign segments. A ratio of
        0 means that no segments will be reassigned. A ratio of 1 means that all segments
        will be reassigned. Numbers in between mean that segments will be reassigned from the
        highest loaded to the lowest loaded host until the target is achieved.'''
        self.consul.kv['127.0.0.1'] = 1024 * 1024
        self.consul.kv['127.0.0.1/seg1'] = 1024
        self.consul.kv['127.0.0.1/seg2'] = 1024
        self.consul.kv['127.0.0.2'] = 1024 * 1024
        self.consul.kv['127.0.0.2/seg3'] = 1024
        self.consul.kv['127.0.0.2/seg4'] = 1024 * 1000
        services = defaultdict(list)
        def register(name, service_id="", address=None, port=0, tags=[], ttl=None):
            services[name].append({ "Node": address })
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
    def test_host_is_registered(self):
        services = defaultdict(list)
        def register(name, service_id="", address=None, port=0, tags=[], ttl=None):
            services[name] = [{ "Node": address }]
        self.consul.agent.service.register = register
        def service(name):
            return services[name]
        self.consul.catalog.service = service
        hostname = 'test.example.com'
        registry = sync.HostRegistry(self.consul)
        self.assertEqual(registry.host_is_registered("127.0.0.1"), False)
        registry.register('trough-nodes', service_id='trough/nodes/%s' % hostname, tags=[hostname])
        self.assertEqual(registry.host_is_registered("127.0.0.1"), True)
    def test_deregister(self):
        services = defaultdict(list)
        def register(name, service_id="", address=None, port=0, tags=[], ttl=None):
            services[name] = [{ "Node": address }]
        self.consul.agent.service.register = register
        def service(name):
            return services[name]
        self.consul.catalog.service = service
        def deregister(service_id):
            for key in [k for k in services.keys()]:
                del services[key]
        self.consul.agent.service.deregister = deregister
        hostname = 'test.example.com'
        registry = sync.HostRegistry(self.consul)
        registry.register('trough-nodes', service_id='trough/nodes/%s' % hostname, tags=[hostname])
        self.assertEqual(registry.host_is_registered("127.0.0.1"), True)
        registry.deregister('trough-nodes', service_id='trough/nodes/%s' % hostname)
        self.assertEqual(registry.host_is_registered("127.0.0.1"), False)
    def test_reset_health_check(self):
        ttls_passed = []
        def ttl_pass(service_id):
            ttls_passed.append(service_id)
        self.consul.agent.check.ttl_pass = ttl_pass
        hostname = 'test.example.com'
        registry = sync.HostRegistry(self.consul)
        registry.reset_health_check('nodes', hostname)
        self.assertEqual(ttls_passed, ['service:trough/nodes/test.example.com'])
    def test_assign(self):
        registry = sync.HostRegistry(self.consul)
        segment = sync.Segment(self.consul, '123456', 100, registry)
        registry.assign('localhost', segment)
        self.assertEqual(self.consul.kv['localhost/123456'], 100)
    def test_unassign(self):
        registry = sync.HostRegistry(self.consul)
        segment = sync.Segment(self.consul, '123456', 100, registry)
        registry.assign('localhost', segment)
        registry.unassign('localhost', segment)
        self.assertEqual(self.consul.kv.get('localhost/123456'), None)
    def test_set_quota(self):
        registry = sync.HostRegistry(self.consul)
        registry.set_quota('localhost', 1024 * 1024)
        self.assertEqual(self.consul.kv['localhost'], 1024 * 1024)
    def test_segments_for_host(self):
        registry = sync.HostRegistry(self.consul)
        segment = sync.Segment(self.consul, '123456', 100, registry)
        registry.assign('localhost', segment)
        output = registry.segments_for_host('localhost')
        self.assertEqual(output[0].id, '123456')
        registry.unassign('localhost', segment)
        output = registry.segments_for_host('localhost')
        self.assertEqual(output, [])

class TestMasterSyncController(unittest.TestCase):
    def setUp(self):
        self.consul = mock.Mock()
        class D(dict):
            def find(self, arg, separator=None):
                output = {}
                for key in self.keys():
                    if key.startswith(arg):
                        output[key] = self[key]
                if separator:
                    return [val for val in output.keys()]
                else:
                    return output
        self.consul.kv = D({})
        self.registry = sync.HostRegistry(self.consul)
        self.snakebite_client = mock.Mock()
    def test_hold_election(self):
        services = {}
        def service(id):
            return services.get(id, [])
        self.consul.catalog.service = service
        controller = sync.MasterSyncController(consul=self.consul, registry=self.registry, snakebite_client=self.snakebite_client)
        output = controller.hold_election()
        self.assertEqual(output, True)
        output = controller.hold_election()
        self.assertEqual(output, True)
        services = {'trough-sync-master': [{ 'Node': 'nope' }]}
        output = controller.hold_election()
        self.assertEqual(output, False)
    def test_wait_to_become_leader(self):
        THREAD_DELAY = 0.1
        services = {'trough-sync-master': [{ 'Node': 'nope' }]}
        def service(id):
            return services.get(id, [])
        self.consul.catalog.service = service
        controller = sync.MasterSyncController(consul=self.consul, registry=self.registry, snakebite_client=self.snakebite_client)
        def make_leader():
            time.sleep(THREAD_DELAY)
            del services['trough-sync-master']
        t = threading.Thread(target=make_leader)
        t.start()
        begin = datetime.datetime.now()
        controller.wait_to_become_leader()
        end = datetime.datetime.now()
        thread_jitter = 0.05
        runtime = end - begin
        mintime = THREAD_DELAY - (settings['ELECTION_CYCLE'] + thread_jitter)
        maxtime = THREAD_DELAY + (settings['ELECTION_CYCLE'] + thread_jitter)
        self.assertTrue(mintime <= runtime.total_seconds() <= maxtime)
    def test_wait_for_hosts(self):
        THREAD_DELAY = 0.1
        services = defaultdict(list)
        def register(name, service_id="", address=None, port=0, tags=[], ttl=None):
            services[name] = [{ "Node": address }]
        self.consul.agent.service.register = register
        def service(name):
            return services[name]
        self.consul.catalog.service = service
        hostname = 'test.example.com'
        controller = sync.MasterSyncController(consul=self.consul, registry=self.registry, snakebite_client=self.snakebite_client)
        def register_host():
            time.sleep(THREAD_DELAY)
            self.registry.register('trough-nodes', service_id='trough/nodes/%s' % hostname, tags=[hostname])
        t = threading.Thread(target=register_host)
        t.start()
        begin = datetime.datetime.now()
        controller.wait_for_hosts()
        end = datetime.datetime.now()
        thread_jitter = 0.05
        runtime = end - begin
        mintime = THREAD_DELAY - (settings['ELECTION_CYCLE'] + thread_jitter)
        maxtime = THREAD_DELAY + (settings['ELECTION_CYCLE'] + thread_jitter)
        self.assertTrue(mintime <= runtime.total_seconds() <= maxtime)
    def test_get_segment_file_list(self):
        def ls(*args, **kwargs):
            yield {'length': 1024 * 1000, 'path': '/seg1.sqlite'}
        self.snakebite_client.ls = ls
        controller = sync.MasterSyncController(consul=self.consul, registry=self.registry, snakebite_client=self.snakebite_client)
        listing = controller.get_segment_file_list()
        for item in listing:
            self.assertEqual(item['length'], 1024*1000)
    def test_assign_segments(self):
        def ls(*args, **kwargs):
            yield {'length': 1024 * 1000, 'path': '/seg1.sqlite'}
        self.snakebite_client.ls = ls

        services = defaultdict(list)
        def register(name, service_id="", address=None, port=0, tags=[], ttl=None):
            services[name] = [{ "Node": address }]
        self.consul.agent.service.register = register
        def service(name):
            return services[name]
        self.consul.catalog.service = service

        hostname = 'test.example.com'
        self.registry.register('trough-nodes', service_id='trough/nodes/%s' % hostname, tags=[hostname])
        self.registry.set_quota('127.0.0.1', 1024 * 1024)

        controller = sync.MasterSyncController(consul=self.consul, registry=self.registry, snakebite_client=self.snakebite_client)
        controller.assign_segments()
        self.assertEqual(self.consul.kv['127.0.0.1/seg1'], 1024 * 1000)
    def test_rebalance_hosts(self):
        self.consul.kv['127.0.0.1'] = 1024 * 1024
        self.consul.kv['127.0.0.1/seg1'] = 1024
        self.consul.kv['127.0.0.1/seg2'] = 1024
        self.consul.kv['127.0.0.2'] = 1024 * 1024
        self.consul.kv['127.0.0.2/seg3'] = 1024
        self.consul.kv['127.0.0.2/seg4'] = 1024 * 1000
        self.consul.kv['127.0.0.3'] = 1024 * 1024
        services = defaultdict(list)
        def register(name, service_id="", address=None, port=0, tags=[], ttl=None):
            services[name].append({ "Node": address })
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
        controller = sync.MasterSyncController(consul=self.consul, registry=self.registry, snakebite_client=self.snakebite_client)
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
        controller = sync.MasterSyncController(consul=self.consul, registry=self.registry, snakebite_client=self.snakebite_client)
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
        controller = sync.MasterSyncController(consul=self.consul, registry=self.registry, snakebite_client=self.snakebite_client)
        controller.rebalance_hosts()
        self.assertEqual(len(self.consul.kv.find('127.0.0.3')), 1)

    def test_sync(self):
        pass

class TestLocalSyncController(unittest.TestCase):
    def setUp(self):
        self.consul = mock.Mock()
        class D(dict):
            def find(self, arg, separator=None):
                output = {}
                for key in self.keys():
                    if key.startswith(arg):
                        output[key] = self[key]
                if separator:
                    return [val for val in output.keys()]
                else:
                    return output
        self.consul.kv = D({})
        self.registry = sync.HostRegistry(self.consul)
        self.snakebite_client = mock.Mock()
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
        controller = sync.LocalSyncController(consul=self.consul, registry=self.registry, snakebite_client=self.snakebite_client)
        segment = sync.Segment(self.consul, 'test-segment', 100, self.registry)
        output = controller.check_segment_matches_hdfs(segment)
        self.assertEqual(output, True)
        test_value = 50
        segment = sync.Segment(self.consul, 'test-segment', 100, self.registry)
        output = controller.check_segment_matches_hdfs(segment)
        self.assertEqual(output, False)
    # don't log out the error message on error test below.
    @mock.patch("trough.sync.logging.error")
    def test_copy_segment_from_hdfs(self, error):
        results = [{'error': 'test error'}]
        def copyToLocal(*args, **kwargs):
            for result in results:
                yield result
        self.snakebite_client.copyToLocal = copyToLocal
        controller = sync.LocalSyncController(consul=self.consul, registry=self.registry, snakebite_client=self.snakebite_client)
        segment = sync.Segment(self.consul, 'test-segment', 100, self.registry)
        with self.assertRaises(Exception):
            output = controller.copy_segment_from_hdfs(segment)
        results = [{}]
        output = controller.copy_segment_from_hdfs(segment)
        self.assertEqual(output, True)
    def test_ensure_registered(self):
        hosts = [{'Node': 'read01'}]
        def get_hosts(*args, **kwargs):
            return hosts
        called = [False]
        def register(*args, **kwargs):
            called[0] = True
        self.registry.get_hosts = get_hosts
        self.registry.register = register
        controller = sync.LocalSyncController(consul=self.consul, registry=self.registry, snakebite_client=self.snakebite_client)
        controller.ensure_registered()
        # our host is already registered. `register` should not be called.
        self.assertEqual(called[0], False)
        hosts = []
        controller.ensure_registered()
        self.assertEqual(called[0], True)
    def test_reset_health_check(self):
        called = [False]
        def check_call(*args, **kwargs):
            called[0] = True
        self.registry.reset_health_check = check_call
        controller = sync.LocalSyncController(consul=self.consul, registry=self.registry, snakebite_client=self.snakebite_client)
        controller.reset_health_check()
        self.assertEqual(called[0], True)
    @mock.patch("trough.sync.logging.error")
    def test_sync_segments(self, error):
        segments = [sync.Segment(self.consul, 'test-segment', 100, self.registry)]
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
        self.registry.register = check_call
        self.registry.reset_health_check = check_call
        controller = sync.LocalSyncController(consul=self.consul, registry=self.registry, snakebite_client=self.snakebite_client)
        with self.assertRaises(Exception):
            controller.sync_segments()
        results = [{}]
        controller.sync_segments()
        self.assertEqual(called, [True, True])
    def test_provision_writable_segment(self):
        test_segment = sync.Segment(consul=self.consul, segment_id='test', registry=self.registry, size=0)
        test_path = test_segment.local_path()
        if os.path.isfile(test_path):
            os.remove(test_path)
        called = []
        def check_call(*args, **kwargs):
            called.append(True)
        self.registry.register = check_call
        self.registry.reset_health_check = check_call
        controller = sync.LocalSyncController(consul=self.consul, registry=self.registry, snakebite_client=self.snakebite_client)
        controller.provision_writable_segment('test')
        self.assertEqual(os.path.isfile(test_path), True)
        os.remove(test_path)


if __name__ == '__main__':
    unittest.main()