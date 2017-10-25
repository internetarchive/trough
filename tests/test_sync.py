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
import rethinkdb as r
import random
import string
import tempfile

random_db = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))

class TestSegment(unittest.TestCase):
    def setUp(self):
        self.rethinker = doublethink.Rethinker(db=random_db, servers=settings['RETHINKDB_HOSTS'])
        self.services = doublethink.ServiceRegistry(self.rethinker)
        self.registry = sync.HostRegistry(rethinker=self.rethinker, services=self.services)
        sync.init(self.rethinker)
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
    #def test_healthy_services_query(self):
    #    sync.healthy_services_query(self.rethinker, 'trough-read')
    def test_readable_copies(self):
        registry = sync.HostRegistry(rethinker=self.rethinker, services=self.services)
        segment = sync.Segment('test-segment',
            services=self.services,
            rethinker=self.rethinker,
            registry=self.registry,
            size=100)
        registry.heartbeat(pool='trough-read',
            node=settings['HOSTNAME'],
            ttl=0.4,
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
        output = segment.provision_local_segment('')
        os.remove(segment.local_path())


class TestHostRegistry(unittest.TestCase):
    def setUp(self):
        self.rethinker = doublethink.Rethinker(db=random_db, servers=settings['RETHINKDB_HOSTS'])
        self.services = doublethink.ServiceRegistry(self.rethinker)
        sync.init(self.rethinker)
        self.rethinker.table("services").delete().run()
        self.rethinker.table("lock").delete().run()
        self.rethinker.table("assignment").delete().run()
    def test_get_hosts(self):
        hostname = 'test.example.com'
        registry = sync.HostRegistry(rethinker=self.rethinker, services=self.services)
        registry.heartbeat(pool='trough-nodes', service_id='trough:nodes:%s' % hostname, node=hostname, ttl=0.6)
        output = registry.get_hosts()
        self.assertEqual(output[0]['node'], "test.example.com")
    def test_heartbeat(self):
        '''This function unusually produces indeterminate output.'''
        hostname = 'test.example.com'
        registry = sync.HostRegistry(rethinker=self.rethinker, services=self.services)
        registry.heartbeat(pool='trough-nodes',
            service_id='trough:nodes:%s' % hostname,
            node=hostname,
            ttl=0.3,
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
        self.rethinker = doublethink.Rethinker(db=random_db, servers=settings['RETHINKDB_HOSTS'])
        self.services = doublethink.ServiceRegistry(self.rethinker)
        self.registry = sync.HostRegistry(rethinker=self.rethinker, services=self.services)
        sync.init(self.rethinker)
        self.snakebite_client = mock.Mock()
        self.rethinker.table("services").delete().run()
        self.rethinker.table("assignment").delete().run()
    def get_foreign_controller(self):
        controller = sync.MasterSyncController(rethinker=self.rethinker,
            services=self.services,
            registry=self.registry)
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
            registry=self.registry)
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
    @mock.patch("trough.sync.client")
    def test_get_segment_file_list(self, snakebite):
        class C:
            def __init__(*args, **kwargs):
                pass
            def ls(*args, **kwargs):
                yield {'length': 1024 * 1000, 'path': '/seg1.sqlite'}
        snakebite.Client = C
        controller = sync.MasterSyncController(
            rethinker=self.rethinker,
            services=self.services,
            registry=self.registry)
        listing = controller.get_segment_file_list()
        called = False
        for item in listing:
            self.assertEqual(item['length'], 1024*1000)
            called = True
        self.assertTrue(called)
    @mock.patch("trough.sync.client")
    def test_assign_segments(self, snakebite):
        class C:
            def __init__(*args, **kwargs):
                pass
            def ls(*args, **kwargs):
                yield {'length': 1024 * 1000, 'path': '/seg1.sqlite'}
        snakebite.Client = C
        hostname = 'test.example.com'
        self.registry.heartbeat(pool='trough-nodes',
            service_id='trough:nodes:%s' % hostname,
            node=hostname,
            ttl=0.3,
            available_bytes=1024*1024)
        controller = self.get_local_controller()
        controller.assign_segments()
        assignments = [asmt for asmt in self.rethinker.table('assignment').filter(r.row['id'] != 'ring-assignments').run()]
        self.assertEqual(len(assignments), 1)
        self.assertEqual(assignments[0]['bytes'], 1024 * 1000)
        self.assertEqual(assignments[0]['hash_ring'], 0)
    @mock.patch("trough.sync.requests")
    def test_provision_writable_segment(self, requests):
        u = []
        d = []
        class Response(object):
            def __init__(self, code, text):
                self.status_code = code
                self.text = text or "Test"
        def p(url, data=None, json=None):
            u.append(url)
            d.append(data or json)
            host = url.split("/")[2].split(":")[0]
            if url == 'http://example4:6112/provision':
                return Response(500, "Test")
            else:
                return Response(200, """{ "url": "http://%s:6222/?segment=testsegment" }""" % host)
        requests.post = p
        self.rethinker.table('services').insert({
            'role': "trough-nodes",
            'node': "example3",
            'segment': "testsegment",
            'ttl': 999,
            'last_heartbeat': r.now(),
        }).run()
        self.rethinker.table('services').insert({
            'id': "trough-read:example2:testsegment",
            'role': "trough-read",
            'node': "example2",
            'segment': "testsegment",
            'ttl': 999,
            'last_heartbeat': r.now(),
        }).run()
        self.rethinker.table('lock').insert({ 
            'id': 'write:lock:testsegment', 
            'node':'example', 
            'segment': 'testsegment' }).run()
        controller = self.get_local_controller()
        # check behavior when lock exists
        output = controller.provision_writable_segment('testsegment')
        self.assertEqual(output['url'], 'http://example:6222/?segment=testsegment')
        # check behavior when readable copy exists
        self.rethinker.table('lock').get('write:lock:testsegment').delete().run()
        output = controller.provision_writable_segment('testsegment')
        self.assertEqual(u[1], 'http://example2:6112/provision')
        self.assertEqual(d[1]['segment'], 'testsegment')
        self.assertEqual(output['url'], 'http://example2:6222/?segment=testsegment')
        # check behavior when only pool of nodes exists
        self.rethinker.table('services').get( "trough-read:example2:testsegment").delete().run()
        output = controller.provision_writable_segment('testsegment')
        self.assertEqual(u[2], 'http://example3:6112/provision')
        self.assertEqual(d[2]['segment'], 'testsegment')
        self.assertEqual(output['url'], 'http://example3:6222/?segment=testsegment')
        self.rethinker.table('services').delete().run()
        self.rethinker.table('services').insert({
            'role': "trough-nodes",
            'node': "example4",
            'segment': "testsegment",
            'ttl': 999,
            'last_heartbeat': r.now(),
        }).run()
        with self.assertRaisesRegex(Exception, 'Received response 500:'):
            output = controller.provision_writable_segment('testsegment')
        self.assertEqual(u[3], 'http://example4:6112/provision')
        self.assertEqual(d[3]['segment'], 'testsegment')
        # check behavior when no nodes in pool?
    def test_sync(self):
        pass

class TestLocalSyncController(unittest.TestCase):
    def setUp(self):
        self.rethinker = doublethink.Rethinker(db=random_db, servers=settings['RETHINKDB_HOSTS'])
        self.services = doublethink.ServiceRegistry(self.rethinker)
        self.registry = sync.HostRegistry(rethinker=self.rethinker, services=self.services)
        self.snakebite_client = mock.Mock()
        self.rethinker.table("services").delete().run()
    def make_fresh_controller(self):
        return sync.LocalSyncController(rethinker=self.rethinker,
            services=self.services,
            registry=self.registry)
    # v don't log out the error message on error test below.
    @mock.patch("trough.sync.client")
    @mock.patch("trough.sync.logging.error")
    def test_copy_segment_from_hdfs(self, error, snakebite):
        results = [{'error': 'test error'}]
        class C:
            def __init__(*args, **kwargs):
                pass
            def copyToLocal(*args, **kwargs):
                for result in results:
                    yield result
        snakebite.Client = C
        controller = self.make_fresh_controller()
        segment = sync.Segment('test-segment',
            services=self.services,
            rethinker=self.rethinker,
            registry=self.registry,
            size=100,
            remote_path='/fake/remote/path')
        with self.assertRaises(Exception):
            output = controller.copy_segment_from_hdfs(segment)
        results = [{}]
        output = controller.copy_segment_from_hdfs(segment)
        self.assertEqual(output, True)
    def test_heartbeat(self):
        controller = self.make_fresh_controller()
        controller.heartbeat()
        output = [svc for svc in self.rethinker.table('services').run()]
        self.assertEqual(output[0]['node'], 'test01')
        self.assertEqual(output[0]['first_heartbeat'], output[0]['last_heartbeat'])

    @mock.patch("trough.sync.client")
    def test_sync_discard_uninteresting_segments(self, snakebite):
        with tempfile.TemporaryDirectory() as tmp_dir:
            controller = self.make_fresh_controller()
            controller.local_data = tmp_dir
            sync.init(self.rethinker)
            assert controller.healthy_service_ids == set()
            controller.sync()
            assert controller.healthy_service_ids == set()
            controller.healthy_service_ids.add('trough-read:test01:1')
            controller.healthy_service_ids.add('trough-read:test01:2')
            controller.healthy_service_ids.add('trough-write:test01:2')
            controller.sync()
            assert controller.healthy_service_ids == set()

            # make segment 3 a segment of interest
            with open(os.path.join(tmp_dir, '3.sqlite'), 'wb'):
                pass
            controller.healthy_service_ids.add('trough-read:test01:1')
            controller.healthy_service_ids.add('trough-read:test01:3')
            controller.healthy_service_ids.add('trough-write:test01:3')
            controller.sync()
            assert controller.healthy_service_ids == {'trough-read:test01:3', 'trough-write:test01:3'}

    @mock.patch("trough.sync.client")
    def test_sync_segment_freshness(self, snakebite):
        self.rethinker.table('lock').delete().run()
        self.rethinker.table('assignment').delete().run()
        self.rethinker.table('services').delete().run()
        with tempfile.TemporaryDirectory() as tmp_dir:
            controller = self.make_fresh_controller()
            controller.local_data = tmp_dir
            sync.init(self.rethinker)
            assert controller.healthy_service_ids == set()
            # make segment 4 a segment of interest
            with open(os.path.join(tmp_dir, '4.sqlite'), 'wb'):
                pass
            controller.sync()
            assert controller.healthy_service_ids == {'trough-read:test01:4'}

            # create a write lock
            lock = sync.Lock.acquire(self.rethinker, 'trough-write:test01:4', {'segment':'4'})
            controller.sync()
            assert controller.healthy_service_ids == {'trough-read:test01:4', 'trough-write:test01:4'}
            locks = list(self.rethinker.table('lock').run())

            assert len(locks) == 1
            assert locks[0]['id'] == 'trough-write:test01:4'

        self.rethinker.table('lock').delete().run()
        self.rethinker.table('assignment').delete().run()

        class C:
            def __init__(*args, **kwargs):
                pass
            def ls(*args, **kwargs):
                yield {'length': 1024 * 1000, 'path': '/5.sqlite', 'modification_time': 1}
            def copyToLocal(*args, **kwargs):
                return [{'error':''}]
        snakebite.Client = C

        # clean slate
        with tempfile.TemporaryDirectory() as tmp_dir:
            controller = self.make_fresh_controller()
            # create an assignment without a local segment
            assignment = sync.Assignment(self.rethinker, d={
                'hash_ring': 'a', 'node': 'test01', 'segment': '5',
                'assigned_on': r.now(), 'remote_path': '/5.sqlite', 'bytes': 0})
            assignment.save()
            lock = sync.Lock.acquire(self.rethinker, 'trough-write:test01:5', {'segment':'5'})
            assert len(list(self.rethinker.table('lock').run())) == 1
            controller.healthy_service_ids.add('trough-write:test01:5')
            controller.healthy_service_ids.add('trough-read:test01:5')
            controller.sync()
            assert controller.healthy_service_ids == {'trough-read:test01:5'}
            assert list(self.rethinker.table('lock').run()) == []

    def test_periodic_heartbeat(self):
        controller = self.make_fresh_controller()
        controller.sync_loop_timing = 1
        controller.healthy_service_ids = {'trough-read:test01:id0', 'trough-read:test01:id1'}
        assert set(self.rethinker.table('services')['id'].run()) == set()

        # first time it inserts individual services
        heartbeats_after = doublethink.utcnow()
        healthy_service_ids = controller.periodic_heartbeat()
        assert set(healthy_service_ids) == {'trough-read:test01:id0', 'trough-read:test01:id1'}
        assert set(self.rethinker.table('services')['id'].run()) == {'trough-nodes:test01:None', 'trough-read:test01:id0', 'trough-read:test01:id1'}
        for svc in self.rethinker.table('services').run():
            assert svc['last_heartbeat'] > heartbeats_after

        # subsequently updates existing services in one bulk query
        heartbeats_after = doublethink.utcnow()
        healthy_service_ids = controller.periodic_heartbeat()
        assert set(healthy_service_ids) == {'trough-read:test01:id0', 'trough-read:test01:id1'}
        assert set(self.rethinker.table('services')['id'].run()) == {'trough-nodes:test01:None', 'trough-read:test01:id0', 'trough-read:test01:id1'}
        for svc in self.rethinker.table('services').run():
            assert svc['last_heartbeat'] > heartbeats_after

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
        controller = self.make_fresh_controller()
        controller.provision_writable_segment('test')
        self.assertEqual(os.path.isfile(test_path), True)
        os.remove(test_path)

if __name__ == '__main__':
    unittest.main()
