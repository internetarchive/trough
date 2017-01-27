import unittest
from unittest import mock
from trough import sync

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
    def test_up_copies(self):
        consul = mock.Mock()
        consul.catalog.service = mock.Mock(return_value=[{"Node": "test-segment.test-node"}])
        segment = sync.Segment(consul, 'test-segment', 100, self.registry)
        output = segment.up_copies()
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
        pass

class TestHostRegistry(unittest.TestCase):
    def setUp(self):
        self.consul = mock.Mock()
        self.consul.kv = {}
    def test_get_hosts(self):
        pass
    def test_look_for_hosts(self):
        pass
    def test_host_load(self):
        pass
    def test_host_bytes_remaining(self):
        pass
    def test_underloaded_hosts(self):
        pass
    def test_host_is_advertised(self):
        pass
    def test_advertise(self):
        pass
    def test_health_check(self):
        pass
    def test_create_health_check(self):
        pass
    def test_reset_health_check(self):
        pass
    def test_assign(self):
        pass
    def test_unassign(self):
        pass
    def set_quota(self):
        pass
    def segments_for_host(self):
        pass

class TestSyncMasterController(unittest.TestCase):
    def test_hold_election(self):
        pass
    def test_wait_to_become_leader(self):
        pass
    def test_wait_for_hosts(self):
        pass
    def test_get_segment_file_list(self):
        pass
    def test_assign_segments(self):
        pass
    def test_rebalance_hosts(self):
        pass
    def test_sync(self):
        pass

class TestSyncLocalController(unittest.TestCase):
    def test_check_segment_exists(self):
        pass
    def test_check_segment_matches_hdfs(self):
        pass
    def test_copy_segment_from_hdfs(self):
        pass
    def test_ensure_advertised(self):
        pass
    def test_ensure_health_check(self):
        pass
    def test_reset_health_check(self):
        pass
    def test_sync_segments(self):
        pass
    def test_sync(self):
        pass

if __name__ == '__main__':
    unittest.main()