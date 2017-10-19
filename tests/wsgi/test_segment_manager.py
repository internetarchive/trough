import pytest
from trough.wsgi.segment_manager import local
import ujson

@pytest.fixture(scope="module")
def segment_manager_local():
    local.testing = True
    return local.test_client()

def test_simple_provision(segment_manager_local):
    result = segment_manager_local.get('/')
    assert result.status == '405 METHOD NOT ALLOWED'

    result = segment_manager_local.post('/', data='some_segment')
    assert result.status_code == 200
    assert b''.join(result.response) == b'http://read01:6222/?segment=some_segment'

def test_provision(segment_manager_local):
    result = segment_manager_local.get('/provision')
    assert result.status == '405 METHOD NOT ALLOWED'

    result = segment_manager_local.post(
            '/provision', content_type='application/json',
            data=ujson.dumps({'segment':'some_segment'}))
    assert result.status_code == 200
    result_bytes = b''.join(result.response)
    result_dict = ujson.loads(result_bytes) # ujson accepts bytes! 😻
    assert result_dict['write_url'] == 'http://read01:6222/?segment=some_segment'
