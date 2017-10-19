import pytest
from trough.wsgi.segment_manager import local

@pytest.fixture(scope="module")
def segment_manager_local():
    local.testing = True
    return local.test_client()

def test_simple_provision(segment_manager_local):
    result = segment_manager_local.get('/')
    assert result.status == '405 METHOD NOT ALLOWED'

    result = segment_manager_local.post('/', data='some_segment')
    assert result.status_code == 200
    # assert result

def test_provision(segment_manager_local):
    result = segment_manager_local.get('/provision')
    assert result.status == '405 METHOD NOT ALLOWED'

