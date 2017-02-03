import unittest
from unittest import mock
import trough
import json
import sqlite3
from tempfile import NamedTemporaryFile
import os

class TestReadServer(unittest.TestCase):
    def setUp(self):
        self.server = trough.read.ReadServer()
        self.server(mock.Mock, mock.Mock)
    def test_empty_read(self):
        database_file = NamedTemporaryFile()
        connection = sqlite3.connect(database_file.name)
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, test varchar(4));')
        # no inserts!
        connection.commit()

        segment = mock.Mock()
        segment.segment_path = lambda: database_file.name

        output = b""
        for part in self.server.read(segment, b'SELECT * FROM "test";'):
            output += part
        output = json.loads(output.decode('utf-8'))
        database_file.close()
        cursor.close()
        connection.close()
        self.assertEqual(output, [])
    def test_read(self):
        database_file = NamedTemporaryFile()
        connection = sqlite3.connect(database_file.name)
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, test varchar(4));')
        cursor.execute('INSERT INTO test (test) VALUES ("test");')
        connection.commit()
        output = b""

        segment = mock.Mock()
        segment.segment_path = lambda: database_file.name

        for part in self.server.read(segment, b'SELECT * FROM "test";'):
            output += part
        output = json.loads(output.decode('utf-8'))
        cursor.close()
        connection.close()
        database_file.close()
        self.assertEqual(output, [{'id': 1, 'test': 'test'}])
    def test_write_failure(self):
        database_file = NamedTemporaryFile()
        connection = sqlite3.connect(database_file.name)
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, test varchar(4));')
        cursor.execute('INSERT INTO test (test) VALUES ("test");')
        connection.commit()
        output = b""

        segment = mock.Mock()
        segment.segment_path = lambda: database_file.name

        with self.assertRaises(Exception):
            for item in self.server.read(segment, b'INSERT INTO test (test) VALUES ("test");'):
                print("item:", item)
        database_file.close()
        cursor.close()
        connection.close()

if __name__ == '__main__':
    unittest.main()