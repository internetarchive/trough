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
    result = segment_manager_server.get('/promote')
    assert result.status == '405 METHOD NOT ALLOWED'

    assert False
    # provision a test segment for write
    # result = segment_manager_server.post(
    #         '/provision', content_type='application/json',
    #         data=ujson.dumps({'segment':'test_provision_segment'}))
    # assert result.status_code == 200
    # assert result.mimetype == 'application/json'
    # result_bytes = b''.join(result.response)
    # result_dict = ujson.loads(result_bytes)
    # assert result_dict['write_url'].endswith(':6222/?segment=test_provision_segment')

    # # now it has already been provisioned
    # result = segment_manager_server.post('/promote', data='test_simple_provision_segment')
    # assert result.status_code == 200
    # assert result.mimetype == 'text/plain'
    # assert b''.join(result.response).endswith(b':6222/?segment=test_simple_provision_segment')

