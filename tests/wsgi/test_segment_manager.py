import pytest
from trough.wsgi.segment_manager import app

@pytest.fixture(scope="module")
def segment_manager_app():
    app.testing = True
    return app.test_client()

def test_simple_provision(segment_manager_app):
    result = segment_manager_app.get('/')
    assert result.status == '405 METHOD NOT ALLOWED'

    import pdb; pdb.set_trace()
    result = segment_manager_app.post('/', data='some_segment')
    assert result.status_code == 200
    # assert result

def test_provision(segment_manager_app):
    result = segment_manager_app.get('/provision')
    assert result.status == '405 METHOD NOT ALLOWED'

