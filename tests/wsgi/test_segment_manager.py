import pytest
from trough.wsgi.segment_manager import server
import ujson
from trough.settings import settings
import doublethink
import rethinkdb as r
import requests # :-\ urllib3?
import hdfs3
import time
import tempfile
import os
import sqlite3

@pytest.fixture(scope="module")
def segment_manager_server():
    server.testing = True
    return server.test_client()

def test_simple_provision(segment_manager_server):
    result = segment_manager_server.get('/')
    assert result.status == '405 METHOD NOT ALLOWED'

    # hasn't been provisioned yet
    result = segment_manager_server.post('/', data='test_simple_provision_segment')
    assert result.status_code == 200
    assert result.mimetype == 'text/plain'
    assert b''.join(result.response).endswith(b':6222/?segment=test_simple_provision_segment')

    # now it has already been provisioned
    result = segment_manager_server.post('/', data='test_simple_provision_segment')
    assert result.status_code == 200
    assert result.mimetype == 'text/plain'
    assert b''.join(result.response).endswith(b':6222/?segment=test_simple_provision_segment')

def test_provision(segment_manager_server):
    result = segment_manager_server.get('/provision')
    assert result.status == '405 METHOD NOT ALLOWED'

    # hasn't been provisioned yet
    result = segment_manager_server.post(
            '/provision', content_type='application/json',
            data=ujson.dumps({'segment':'test_provision_segment'}))
    assert result.status_code == 200
    assert result.mimetype == 'application/json'
    result_bytes = b''.join(result.response)
    result_dict = ujson.loads(result_bytes) # ujson accepts bytes! 😻
    assert result_dict['write_url'].endswith(':6222/?segment=test_provision_segment')

    # now it has already been provisioned
    result = segment_manager_server.post(
            '/provision', content_type='application/json',
            data=ujson.dumps({'segment':'test_provision_segment'}))
    assert result.status_code == 200
    assert result.mimetype == 'application/json'
    result_bytes = b''.join(result.response)
    result_dict = ujson.loads(result_bytes)
    assert result_dict['write_url'].endswith(':6222/?segment=test_provision_segment')

def test_provision_with_schema(segment_manager_server):
    schema = '''CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, test varchar(4));
INSERT INTO test (test) VALUES ("test");'''
    # create a schema by submitting sql
    result = segment_manager_server.put(
            '/schema/test1/sql', content_type='applicaton/sql', data=schema)
    assert result.status_code == 201

    # provision a segment with that schema
    result = segment_manager_server.post(
            '/provision', content_type='application/json',
            data=ujson.dumps({'segment':'test_provision_with_schema_1', 'schema':'test1'}))
    assert result.status_code == 200
    assert result.mimetype == 'application/json'
    result_bytes = b''.join(result.response)
    result_dict = ujson.loads(result_bytes) # ujson accepts bytes! 😻
    assert result_dict['write_url'].endswith(':6222/?segment=test_provision_with_schema_1')

    # get db read url from rethinkdb
    rethinker = doublethink.Rethinker(
            servers=settings['RETHINKDB_HOSTS'], db='trough_configuration')
    query = rethinker.table('services').get_all('test_provision_with_schema_1', index='segment').filter({'role': 'trough-read'}).filter(lambda svc: r.now().sub(svc['last_heartbeat']).lt(svc['ttl'])).order_by('load')[0]
    healthy_segment = query.run()
    read_url = healthy_segment.get('url')
    assert read_url.endswith(':6444/?segment=test_provision_with_schema_1')

    # run a query to check that the schema was used
    sql = 'SELECT * FROM test;'
    with requests.post(read_url, stream=True, data=sql) as response:
        assert response.status_code == 200
        result = ujson.loads(response.text)
        assert result == [{'test': 'test', 'id': 1}]

    # delete the schema from rethinkdb for the sake of other tests
    rethinker = doublethink.Rethinker(
            servers=settings['RETHINKDB_HOSTS'], db='trough_configuration')
    result = rethinker.table('schema').get('test1').delete().run()
    assert result == {'deleted': 1, 'inserted': 0, 'skipped': 0, 'errors': 0, 'unchanged': 0, 'replaced': 0}

def test_schemas(segment_manager_server):
    # initial list of schemas
    result = segment_manager_server.get('/schema')
    assert result.status_code == 200
    assert result.mimetype == 'application/json'
    result_bytes = b''.join(result.response)
    result_list = ujson.loads(result_bytes)
    assert set(result_list) == {'default'}

    # existent schema as json
    result = segment_manager_server.get('/schema/default')
    assert result.status_code == 200
    assert result.mimetype == 'application/json'
    result_bytes = b''.join(result.response)
    result_dict = ujson.loads(result_bytes)
    assert result_dict == {'id': 'default', 'sql': ''}

    # existent schema sql
    result = segment_manager_server.get('/schema/default/sql')
    assert result.status_code == 200
    assert result.mimetype == 'application/sql'
    result_bytes = b''.join(result.response)
    assert result_bytes == b''

    # schema doesn't exist yet
    result = segment_manager_server.get('/schema/schema1')
    assert result.status_code == 404

    # schema doesn't exist yet
    result = segment_manager_server.get('/schema/schema1/sql')
    assert result.status_code == 404

    # bad request: POST not accepted (must be PUT)
    result = segment_manager_server.post('/schema/schema1', data='{}')
    assert result.status_code == 405
    result = segment_manager_server.post('/schema/schema1/sql', data='')
    assert result.status_code == 405

    # bad request: invalid json
    result = segment_manager_server.put(
            '/schema/schema1', data=']]}what the not valid json' )
    assert result.status_code == 400
    assert b''.join(result.response) == b'input could not be parsed as json'

    # bad request: id in json does not match url
    result = segment_manager_server.put(
            '/schema/schema1', data=ujson.dumps({'id': 'schema2', 'sql': ''}))
    assert result.status_code == 400
    assert b''.join(result.response) == b"id in json 'schema2' does not match id in url 'schema1'"

    # bad request: missing sql
    result = segment_manager_server.put(
            '/schema/schema1', data=ujson.dumps({'id': 'schema1'}))
    assert result.status_code == 400
    assert b''.join(result.response) == b"input json has keys {'id'} (should be {'id', 'sql'})"

    # bad request: missing id
    result = segment_manager_server.put(
            '/schema/schema1', data=ujson.dumps({'sql': ''}))
    assert result.status_code == 400
    assert b''.join(result.response) == b"input json has keys {'sql'} (should be {'id', 'sql'})"

    # bad request: invalid sql
    result = segment_manager_server.put(
            '/schema/schema1', data=ujson.dumps({'id': 'schema1', 'sql': 'create create table table blah blooofdjaio'}))
    assert result.status_code == 400
    assert b''.join(result.response) == b'schema sql failed validation: near "create": syntax error'

    # create new schema by submitting sql
    result = segment_manager_server.put(
            '/schema/schema1/sql', content_type='applicaton/sql',
            data='create table foo (bar varchar(100));')
    assert result.status_code == 201

    # get the new schema as json
    result = segment_manager_server.get('/schema/schema1')
    assert result.status_code == 200
    assert result.mimetype == 'application/json'
    result_bytes = b''.join(result.response)
    result_dict = ujson.loads(result_bytes)
    assert result_dict == {'id': 'schema1', 'sql': 'create table foo (bar varchar(100));'}

    # get the new schema as sql
    result = segment_manager_server.get('/schema/schema1/sql')
    assert result.status_code == 200
    assert result.mimetype == 'application/sql'
    result_bytes = b''.join(result.response)
    assert result_bytes == b'create table foo (bar varchar(100));'

    # create new schema by submitting json
    result = segment_manager_server.put(
            '/schema/schema2', content_type='applicaton/sql',
            data=ujson.dumps({'id': 'schema2', 'sql': 'create table schema2_table (foo varchar(100));'}))
    assert result.status_code == 201

    # get the new schema as json
    result = segment_manager_server.get('/schema/schema2')
    assert result.status_code == 200
    assert result.mimetype == 'application/json'
    result_bytes = b''.join(result.response)
    result_dict = ujson.loads(result_bytes)
    assert result_dict == {'id': 'schema2', 'sql': 'create table schema2_table (foo varchar(100));'}

    # get the new schema as sql
    result = segment_manager_server.get('/schema/schema2/sql')
    assert result.status_code == 200
    assert result.mimetype == 'application/sql'
    result_bytes = b''.join(result.response)
    assert result_bytes == b'create table schema2_table (foo varchar(100));'

    # updated list of schemas
    result = segment_manager_server.get('/schema')
    assert result.status_code == 200
    assert result.mimetype == 'application/json'
    result_bytes = b''.join(result.response)
    result_list = ujson.loads(result_bytes)
    assert set(result_list) == {'default', 'schema1', 'schema2'}

    # overwrite schema1 with json api
    result = segment_manager_server.put(
            '/schema/schema1', content_type='applicaton/json',
            data=ujson.dumps({'id': 'schema1', 'sql': 'create table blah (toot varchar(100));'}))
    assert result.status_code == 204

    # get the modified schema as sql
    result = segment_manager_server.get('/schema/schema1/sql')
    assert result.status_code == 200
    assert result.mimetype == 'application/sql'
    result_bytes = b''.join(result.response)
    assert result_bytes == b'create table blah (toot varchar(100));'

    # overwrite schema1 with sql api
    result = segment_manager_server.put(
            '/schema/schema1/sql', content_type='applicaton/sql',
            data='create table haha (hehehe varchar(100));')
    assert result.status_code == 204

    # get the modified schema as json
    result = segment_manager_server.get('/schema/schema1')
    assert result.status_code == 200
    assert result.mimetype == 'application/json'
    result_bytes = b''.join(result.response)
    result_dict = ujson.loads(result_bytes)
    assert result_dict == {'id': 'schema1', 'sql': 'create table haha (hehehe varchar(100));'}

    # updated list of schemas
    result = segment_manager_server.get('/schema')
    assert result.status_code == 200
    assert result.mimetype == 'application/json'
    result_bytes = b''.join(result.response)
    result_list = ujson.loads(result_bytes)
    assert set(result_list) == {'default', 'schema1', 'schema2'}

    # XXX DELETE?

def test_promotion(segment_manager_server):
    hdfs = hdfs3.HDFileSystem(settings['HDFS_HOST'], settings['HDFS_PORT'])

    hdfs.rm(settings['HDFS_PATH'])
    hdfs.mkdir(settings['HDFS_PATH'])

    result = segment_manager_server.get('/promote')
    assert result.status == '405 METHOD NOT ALLOWED'

    # provision a test segment for write
    result = segment_manager_server.post(
            '/provision', content_type='application/json',
            data=ujson.dumps({'segment':'test_promotion'}))
    assert result.status_code == 200
    assert result.mimetype == 'application/json'
    result_bytes = b''.join(result.response)
    result_dict = ujson.loads(result_bytes)
    assert result_dict['write_url'].endswith(':6222/?segment=test_promotion')
    write_url = result_dict['write_url']

    # write something into the db
    sql = ('create table foo (bar varchar(100));\n'
           'insert into foo (bar) values ("testing segment promotion");\n')
    response = requests.post(write_url, sql)
    assert response.status_code == 200

    # shouldn't be anything in hdfs yet...
    expected_remote_path = '%s/tes/test_promotion.sqlite' % settings['HDFS_PATH']
    with pytest.raises(FileNotFoundError):
        hdfs.ls(expected_remote_path, detail=True)

    # now write to the segment and promote it to HDFS
    before = time.time()
    time.sleep(1.5)
    result = segment_manager_server.post(
            '/promote', content_type='application/json',
            data=ujson.dumps({'segment': 'test_promotion'}))
    assert result.status_code == 200
    assert result.mimetype == 'application/json'
    result_bytes = b''.join(result.response)
    result_dict = ujson.loads(result_bytes)
    assert result_dict == {'remote_path': expected_remote_path}

    # let's see if it's hdfs
    listing_after_promotion = hdfs.ls(expected_remote_path, detail=True)
    assert len(listing_after_promotion) == 1
    assert listing_after_promotion[0]['last_mod'] > before

    # grab the file from hdfs and check the content
    # n.b. copy created by sqlitebck may have different size, sha1 etc from orig
    with tempfile.TemporaryDirectory() as tmpdir:
        local_copy = os.path.join(tmpdir, 'test_promotion.sqlite')
        hdfs.get(expected_remote_path, local_copy)
        conn = sqlite3.connect(local_copy)
        cur = conn.execute('select * from foo')
        assert cur.fetchall() == [('testing segment promotion',)]
        conn.close()
