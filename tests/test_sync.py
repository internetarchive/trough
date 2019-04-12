import os
os.environ['TROUGH_SETTINGS'] = os.path.join(os.path.dirname(__file__), "test.conf")

import unittest
from unittest import mock
from trough import sync
from trough.settings import settings
import time
import doublethink
import rethinkdb as r
import random
import string
import tempfile
import logging
from hdfs3 import HDFileSystem
import pytest

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
    def test_new_write_lock(self):
        lock = sync.Lock.load(self.rethinker, 'write:lock:123456')
        if lock:
            lock.release()
        segment = sync.Segment('123456',
            services=self.services,
            rethinker=self.rethinker,
            registry=self.registry,
            size=100)
        lock = segment.new_write_lock()
        with self.assertRaises(Exception):
            segment.new_write_lock()
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
        output = segment.new_write_lock()
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
    def test_unassign(self):
        segment, registry = self.test_assign()
        registry.commit_assignments()
        registry.unassign(registry.assignment_queue._queue[0])
        self.assertEqual(registry.assignment_queue._queue[0]['id'], 'localhost:123456')
        return registry
    def test_commit_unassignments(self):
        registry = self.test_unassign()
        registry.commit_unassignments()
        output = [seg for seg in segment.all_copies()]
        self.assertEqual(output, [])
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
        registry.unassign(asmt)
        registry.commit_unassignments()
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
    def test_get_segment_file_list(self):
        controller = sync.MasterSyncController(
            rethinker=self.rethinker,
            services=self.services,
            registry=self.registry)
        # populate some dirs/files
        hdfs = HDFileSystem(host=controller.hdfs_host, port=controller.hdfs_port)
        hdfs.rm(controller.hdfs_path, recursive=True)
        hdfs.mkdir(controller.hdfs_path)
        hdfs.touch(os.path.join(controller.hdfs_path, '0.txt'))
        hdfs.touch(os.path.join(controller.hdfs_path, '1.sqlite'))
        hdfs.mkdir(os.path.join(controller.hdfs_path, '2.dir'))
        hdfs.touch(os.path.join(controller.hdfs_path, '3.txt'))
        with hdfs.open(os.path.join(controller.hdfs_path, '4.sqlite'), 'wb', replication=1) as f:
            f.write(b'some bytes')
        hdfs.touch('/tmp/5.sqlite')
        listing = controller.get_segment_file_list()
        entry = next(listing)
        assert entry['name'] == os.path.join(controller.hdfs_path, '1.sqlite')
        assert entry['kind'] == 'file'
        assert entry['size'] == 0
        entry = next(listing)
        assert entry['name'] == os.path.join(controller.hdfs_path, '4.sqlite')
        assert entry['kind'] == 'file'
        assert entry['size'] == 10
        with pytest.raises(StopIteration):
            next(listing)
        # clean up after successful test
        hdfs.rm(controller.hdfs_path, recursive=True)
        hdfs.mkdir(controller.hdfs_path)
    def test_assign_segments(self):
        controller = self.get_local_controller()
        hdfs = HDFileSystem(host=controller.hdfs_host, port=controller.hdfs_port)
        hdfs.rm(controller.hdfs_path, recursive=True)
        hdfs.mkdir(controller.hdfs_path)
        with hdfs.open(os.path.join(controller.hdfs_path, '1.sqlite'), 'wb', replication=1) as f:
            f.write(b'x' * 1024)
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
        self.assertEqual(assignments[0]['bytes'], 1024)
        self.assertEqual(assignments[0]['hash_ring'], 0)
        # clean up after successful test
        hdfs.rm(controller.hdfs_path, recursive=True)
        hdfs.mkdir(controller.hdfs_path)
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
        # check behavior when we get a downstream error
        self.rethinker.table('services').delete().run()
        self.rethinker.table('services').insert({
            'role': "trough-nodes",
            'node': "example4",
            'ttl': 999,
            'last_heartbeat': r.now(),
        }).run()
        with self.assertRaisesRegex(Exception, 'Received response 500:'):
            output = controller.provision_writable_segment('testsegment')
        self.assertEqual(u[3], 'http://example4:6112/provision')
        self.assertEqual(d[3]['segment'], 'testsegment')
        # check behavior when node expires
        self.rethinker.table('services').delete().run()
        self.rethinker.table('services').insert({
            'role': "trough-nodes",
            'node': "example6",
            'load': 30,
            'ttl': 1.5,
            'last_heartbeat': r.now(),
        }).run()
        self.rethinker.table('services').insert({
            'role': "trough-nodes",
            'node': "example5",
            'load': 0.01,
            'ttl': 0.2,
            'last_heartbeat': r.now(),
        }).run()
        # example 5 hasn't expired yet
        output = controller.provision_writable_segment('testsegment')
        self.assertEqual(u[4], 'http://example5:6112/provision')
        self.assertEqual(d[4], {'segment': 'testsegment', 'schema': 'default'})
        time.sleep(1)
        # example 5 has expired
        output = controller.provision_writable_segment('testsegment')
        self.assertEqual(u[5], 'http://example6:6112/provision')
        self.assertEqual(d[5], {'segment': 'testsegment', 'schema': 'default'})
        time.sleep(1)
        # example 5 and 6 have expired
        with self.assertRaises(Exception):
            output = controller.provision_writable_segment('testsegment')

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
            def copyToLocal(self, paths, dst, *args, **kwargs):
                for result in results:
                    if not result.get('error'):
                        # create empty dest file
                        with open(dst, 'wb') as f: pass
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

    def test_sync_segment_freshness(self):
        sync.init(self.rethinker)
        with tempfile.TemporaryDirectory() as tmp_dir:
            self.rethinker.table('lock').delete().run()
            self.rethinker.table('assignment').delete().run()
            self.rethinker.table('services').delete().run()
            controller = self.make_fresh_controller()
            controller.local_data = tmp_dir
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

        # clean slate
        with tempfile.TemporaryDirectory() as tmp_dir:
            hdfs = HDFileSystem(host=controller.hdfs_host, port=controller.hdfs_port)
            hdfs.rm(controller.hdfs_path, recursive=True)
            hdfs.mkdir(controller.hdfs_path)
            with hdfs.open(os.path.join(controller.hdfs_path, '5.sqlite'), 'wb', replication=1) as f:
                f.write('y' * 1024)
            self.rethinker.table('lock').delete().run()
            self.rethinker.table('assignment').delete().run()
            self.rethinker.table('services').delete().run()
            controller = self.make_fresh_controller()
            controller.local_data = tmp_dir
            # create an assignment without a local segment
            assignment = sync.Assignment(self.rethinker, d={
                'hash_ring': 'a', 'node': 'test01', 'segment': '5',
                'assigned_on': r.now(), 'bytes': 0,
                'remote_path': os.path.join(controller.hdfs_path, '5.sqlite')})
            assignment.save()
            lock = sync.Lock.acquire(self.rethinker, 'write:lock:5', {'segment':'5'})
            assert len(list(self.rethinker.table('lock').run())) == 1
            controller.healthy_service_ids.add('trough-write:test01:5')
            controller.healthy_service_ids.add('trough-read:test01:5')
            controller.sync()
            assert controller.healthy_service_ids == {'trough-read:test01:5'}
            assert list(self.rethinker.table('lock').run()) == []
            # clean up
            hdfs.rm(controller.hdfs_path, recursive=True)
            hdfs.mkdir(controller.hdfs_path)

        # third case: not assigned, local file exists, is older than hdfs
        # this corresponds to the situation where we have an out-of-date
        # segment on disk that was probably a write segment before it was
        # reassigned when it was pushed upstream
        with tempfile.TemporaryDirectory() as tmp_dir:
            # create a local segment without an assignment
            with open(os.path.join(tmp_dir, '6.sqlite'), 'wb'):
                pass
            time.sleep(2)
            # create file in hdfs with newer timestamp
            hdfs = HDFileSystem(host=controller.hdfs_host, port=controller.hdfs_port)
            hdfs.rm(controller.hdfs_path, recursive=True)
            hdfs.mkdir(controller.hdfs_path)
            with hdfs.open(os.path.join(controller.hdfs_path, '6.sqlite'), 'wb', replication=1) as f:
                f.write('z' * 1024)
            self.rethinker.table('lock').delete().run()
            self.rethinker.table('assignment').delete().run()
            self.rethinker.table('services').delete().run()
            controller = self.make_fresh_controller()
            controller.local_data = tmp_dir
            controller.healthy_service_ids.add('trough-write:test01:6')
            controller.healthy_service_ids.add('trough-read:test01:6')
            controller.sync()
            assert controller.healthy_service_ids == set()
            # clean up
            hdfs.rm(controller.hdfs_path, recursive=True)
            hdfs.mkdir(controller.hdfs_path)

    @mock.patch("trough.sync.client")
    def test_hdfs_resiliency(self, snakebite):
        sync.init(self.rethinker)
        self.rethinker.table('lock').delete().run()
        self.rethinker.table('assignment').delete().run()
        self.rethinker.table('services').delete().run()
        assignment = sync.Assignment(self.rethinker, d={
            'hash_ring': 'a', 'node': 'test01', 'segment': '1',
            'assigned_on': r.now(), 'remote_path': '/1.sqlite', 'bytes': 9})
        assignment.save()
        class C:
            def __init__(*args, **kwargs):
                pass
            def ls(*args, **kwargs):
                yield {'length': 1024 * 1000, 'path': '/1.sqlite', 'modification_time': (time.time() + 1000000) * 1000}
            def copyToLocal(*args, **kwargs):
                return [{'error':'There was a problem...'}]
        snakebite.Client = C
        controller = self.make_fresh_controller()
        controller.sync()
        class C:
            def __init__(*args, **kwargs):
                pass
            def ls(*args, **kwargs):
                yield {'length': 1024 * 1000, 'path': '/1.sqlite', 'modification_time': (time.time() + 1000000) * 1000}
            def copyToLocal(*args, **kwargs):
                def g():
                    raise Exception("HDFS IS DOWN")
                    yield 0
                return g()
        snakebite.Client = C
        controller = self.make_fresh_controller()
        controller.sync()
        class C:
            def __init__(*args, **kwargs):
                pass
            def ls(*args, **kwargs):
                def g():
                    raise Exception("HDFS IS DOWN")
                    yield 0
                return g()
            def copyToLocal(*args, **kwargs):
                def g():
                    raise Exception("HDFS IS DOWN")
                    yield 0
                return g()
        snakebite.Client = C
        controller = self.make_fresh_controller()
        controller.sync()
        self.rethinker.table('lock').delete().run()
        self.rethinker.table('assignment').delete().run()
        self.rethinker.table('services').delete().run()

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

    def test_collect_garbage(self):
        # for each segment file on local disk
        # - segment assigned to me should not be gc'd
        # - segment not assigned to me with healthy service count <= minimum
        #   should not be gc'd
        # - segment not assigned to me with healthy service count == minimum
        #   and no local healthy service entry should be gc'd
        # - segment not assigned to me with healthy service count > minimum
        #   and has local healthy service entry should be gc'd
        with tempfile.TemporaryDirectory() as tmp_dir:
            # create segment file
            segment_id = 'test_collect_garbage'
            filename = '%s.sqlite' % segment_id
            path = os.path.join(tmp_dir, filename)
            with open(path, 'wb'):
                pass
            assert os.path.exists(path)

            # create controller
            controller = self.make_fresh_controller()
            controller.local_data = tmp_dir

            # assign to me
            assignment = sync.Assignment(self.rethinker, d={
                'hash_ring': 'a', 'node': 'test01', 'segment': segment_id,
                'assigned_on': r.now(), 'bytes': 9,
                'remote_path': '/%s.sqlite' % segment_id})
            assignment.save()

            # - segment assigned to me should not be gc'd
            controller.collect_garbage()
            assert os.path.exists(path)

            # - segment not assigned to me with healthy service count <= minimum
            #   should not be gc'd
            assignment.unassign()
            # 0 healthy service ids
            controller.collect_garbage()
            assert os.path.exists(path)
            # 1 healthy service id
            controller.registry.heartbeat(pool='trough-read', node='test01', ttl=600, segment=segment_id)
            controller.collect_garbage()
            assert os.path.exists(path)

            # - segment not assigned to me with healthy service count == minimum
            #   and no local healthy service entry should be gc'd
            # delete service entry
            self.rethinker.table('services').get('trough-read:test01:%s' % segment_id).delete().run()
            controller.registry.heartbeat(pool='trough-read', node='test02', ttl=600, segment=segment_id)
            controller.collect_garbage()
            assert not os.path.exists(path)

            # recreate file
            with open(path, 'wb'):
                pass
            assert os.path.exists(path)

            # - segment not assigned to me with healthy service count > minimum
            #   and has local healthy service entry should be gc'd
            controller.registry.heartbeat(pool='trough-read', node='test01', ttl=600, segment=segment_id)
            controller.registry.heartbeat(pool='trough-read', node='test02', ttl=600, segment=segment_id)
            controller.collect_garbage()
            assert not os.path.exists(path)
            assert not self.rethinker.table('services').get('trough-read:test01:%s' % segment_id).run()
            assert self.rethinker.table('services').get('trough-read:test02:%s' % segment_id).run()


if __name__ == '__main__':
    unittest.main()
