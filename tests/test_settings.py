import unittest
from unittest import mock
from trough import settings
import os

class TestSettings(unittest.TestCase):
    def setUp(self):
        pass
    def test_read_settings(self):
        test_settings = settings.Settings(os.path.join(os.path.dirname(__file__), "test.conf"))
        self.assertEqual(test_settings['TEST_SETTING'], 'test__setting__value')

if __name__ == '__main__':
    unittest.main()