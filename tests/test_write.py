import os
os.environ['TROUGH_LOG_LEVEL'] = 'ERROR'
os.environ['TROUGH_SETTINGS'] = os.path.join(os.path.dirname(__file__), "test.conf")

import unittest
from unittest import mock
from trough import write
import json
import sqlite3
from tempfile import NamedTemporaryFile

class TestWriteServer(unittest.TestCase):
    def setUp(self):
        self.server = write.WriteServer()
    def test_empty_write(self):
        database_file = NamedTemporaryFile()
        segment = mock.Mock()
        segment.segment_path = lambda: database_file.name
        # no inserts!
        output = b""
        with self.assertRaises(Exception):
            output = self.server.write(segment, b'')
        database_file.close()
        self.assertEqual(output, b'')
    def test_read_failure(self):
        database_file = NamedTemporaryFile()
        segment = mock.Mock()
        segment.segment_path = lambda: database_file.name
        connection = sqlite3.connect(database_file.name)
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, test varchar(4));')
        cursor.execute('INSERT INTO test (test) VALUES ("test");')
        connection.commit()
        output = b""
        with self.assertRaises(Exception):
            output = self.server.write(segment, b'SELECT * FROM "test";')
        database_file.close()
    def test_write(self):
        database_file = NamedTemporaryFile()
        segment = mock.Mock()
        segment.local_path = lambda: database_file.name
        output = self.server.write(segment, b'CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, test varchar(4));')
        output = self.server.write(segment, b'INSERT INTO test (test) VALUES ("test");')
        connection = sqlite3.connect(database_file.name)
        cursor = connection.cursor()
        output = cursor.execute('SELECT * FROM test;')
        for row in output:
            output = dict((cursor.description[i][0], value) for i, value in enumerate(row))
        database_file.close()
        self.assertEqual(output, {'id': 1, 'test': 'test'})
    def test_write_failure_to_read_only_segment(self):
        database_file = NamedTemporaryFile()
        segment = mock.Mock()
        segment.segment_path = lambda: database_file.name
        connection = sqlite3.connect(database_file.name)
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, test varchar(4));')
        # set up an environment for uwsgi mock
        env = {}
        env['HTTP_HOST'] = "TEST.host"
        env['wsgi.input'] = mock.Mock()
        env['wsgi.input'].read = lambda: b'INSERT INTO test (test) VALUES ("test")'
        start = mock.Mock()
        output = self.server(env, start)
        self.assertEqual(output, [b"500 Server Error: This node (settings['HOSTNAME']='read01') cannot write to segment 'TEST'. There is no write lock set, or the write lock authorizes another node.\n"])
        database_file.close()

if __name__ == '__main__':
    unittest.main()
