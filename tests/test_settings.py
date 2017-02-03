import os
os.environ['TROUGH_LOG_LEVEL'] = 'ERROR'
os.environ['TROUGH_SETTINGS'] = os.path.join(os.path.dirname(__file__), "test.conf")
 
from trough.settings import settings
import unittest
from unittest import mock

class TestSettings(unittest.TestCase):
    def test_read_settings(self):
        self.assertEqual(settings['TEST_SETTING'], 'test__setting__value')

if __name__ == '__main__':
    unittest.main()