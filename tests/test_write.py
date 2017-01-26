import unittest
from unittest import mock
from trough import write
import json
import sqlite3
from tempfile import NamedTemporaryFile

class TestReadServer(unittest.TestCase):
    def setUp(self):
        self.server = write.WriteServer(mock.Mock())
    def test_empty_write(self):
        database_file = NamedTemporaryFile()
        # no inserts!
        output = b""
        with self.assertRaises(Exception):
            output = self.server.write(database_file.name, b'')
        database_file.close()
        self.assertEqual(output, b'')
    def test_read_failure(self):
        database_file = NamedTemporaryFile()
        connection = sqlite3.connect(database_file.name)
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, test varchar(4));')
        cursor.execute('INSERT INTO test (test) VALUES ("test");')
        connection.commit()
        output = b""
        with self.assertRaises(Exception):
            output = self.server.write(database_file.name, b'SELECT * FROM "test";')
        database_file.close()
    def test_write(self):
        database_file = NamedTemporaryFile()
        output = self.server.write(database_file.name, b'CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, test varchar(4));')
        print(output)
        output = self.server.write(database_file.name, b'INSERT INTO test (test) VALUES ("test");')
        print(output)
        connection = sqlite3.connect(database_file.name)
        cursor = connection.cursor()
        output = cursor.execute('SELECT * FROM test;')
        for row in output:
            output = dict((cursor.description[i][0], value) for i, value in enumerate(row))
        database_file.close()
        self.assertEqual(output, {'id': 1, 'test': 'test'})

if __name__ == '__main__':
    unittest.main()