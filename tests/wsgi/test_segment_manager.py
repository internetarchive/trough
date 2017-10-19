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
    assert b''.join(result.response).endswith(b':6222/?segment=test_simple_provision_segment')

    # now it has already been provisioned
    result = segment_manager_server.post('/', data='test_simple_provision_segment')
    assert result.status_code == 200
    assert b''.join(result.response).endswith(b':6222/?segment=test_simple_provision_segment')

def test_provision(segment_manager_server):
    result = segment_manager_server.get('/provision')
    assert result.status == '405 METHOD NOT ALLOWED'

    # hasn't been provisioned yet
    result = segment_manager_server.post(
            '/provision', content_type='application/json',
            data=ujson.dumps({'segment':'test_provision_segment'}))
    assert result.status_code == 200
    result_bytes = b''.join(result.response)
    result_dict = ujson.loads(result_bytes) # ujson accepts bytes! 😻
    assert result_dict['write_url'].endswith(':6222/?segment=test_provision_segment')

    # now it has already been provisioned
    result = segment_manager_server.post(
            '/provision', content_type='application/json',
            data=ujson.dumps({'segment':'test_provision_segment'}))
    assert result.status_code == 200
    result_bytes = b''.join(result.response)
    result_dict = ujson.loads(result_bytes)
    assert result_dict['write_url'].endswith(':6222/?segment=test_provision_segment')

# def test_schemas(segment_manager_server)
