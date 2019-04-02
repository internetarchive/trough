import trough
from trough.settings import settings, init_worker

init_worker()

# setup uwsgi endpoint
application = trough.read.ReadServer()

if __name__ == '__main__':
    from wsgiref.simple_server import make_server
    server = make_server('', 6444, application)
    server.serve_forever()

