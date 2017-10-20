import pytest
from trough.wsgi.segment_manager import server
import ujson

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

def test_schemas(segment_manager_server):
    # initial list of schemas
    result = segment_manager_server.get('/schema')
    assert result.status_code == 200
    assert result.mimetype == 'application/json'
    result_bytes = b''.join(result.response)
    result_list = ujson.loads(result_bytes)
    assert set(result_list) == {'default'}

    # existent schema (as sql)
    result = segment_manager_server.get('/schema/default')
    assert result.status_code == 200
    assert result.mimetype == 'application/sql'
    result_bytes = b''.join(result.response)
    assert result_bytes == b''

    # existent schema (as json)
    result = segment_manager_server.get(
            '/schema/default', headers={'Accept': 'application/json'})
    assert result.status_code == 200
    assert result.mimetype == 'application/json'
    result_bytes = b''.join(result.response)
    result_dict = ujson.loads(result_bytes)
    assert result_dict == {'id': 'default', 'sql': ''}

    # schema doesn't exist yet
    result = segment_manager_server.get('/schema/schema1')
    assert result.status_code == 404

    # create new schema
    result = segment_manager_server.post(
            '/schema/schema1', 'create table foo (bar varchar(100));')
    assert result.status_code == 201

    # updated list of schemas
    result = segment_manager_server.get('/schema')
    assert result.status_code == 200
    assert result.mimetype == 'application/json'
    result_bytes = b''.join(result.response)
    result_list = ujson.loads(result_bytes)
    assert set(result_list) == {'default', 'schema1'}

    # get the new schema (as sql)
    result = segment_manager_server.get('/schema/schema1')
    assert result.status_code == 200
    assert result.mimetype == 'application/json'
    result_bytes = b''.join(result.response)
    result_dict = ujson.loads(result_bytes)
    assert result_dict == {'id': 'schema1', 'sql': 'create table foo (bar varchar(100));'}

    # get the new schema (as json)
    result = segment_manager_server.get('/schema/schema1', )
    assert result.status_code == 200
    assert result.mimetype == 'application/json'
    result_bytes = b''.join(result.response)
    result_dict = ujson.loads(result_bytes)
    assert result_dict == {'id': 'schema1', 'sql': 'create table foo (bar varchar(100));'}

    # overwrite the schema we just created
    result = segment_manager_server.post(
            '/schema/schema1', 'create table bar (baz varchar(100));')
    assert result.status_code == 201

    # get the modified schema
    result = segment_manager_server.get('/schema/schema1')
    assert result.status_code == 200
    assert result.mimetype == 'application/json'
    result_bytes = b''.join(result.response)
    result_dict = ujson.loads(result_bytes)
    assert result_dict == {'id': 'schema1', 'sql': 'create table bar (baz varchar(100));'}

    # updated list of schemas
    result = segment_manager_server.get('/schema')
    assert result.status_code == 200
    assert result.mimetype == 'application/json'
    result_bytes = b''.join(result.response)
    result_list = ujson.loads(result_bytes)
    assert set(result_list) == {'default', 'schema1'}

