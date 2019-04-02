import trough
from trough.settings import settings, init_worker

init_worker()

# setup uwsgi endpoint
application = trough.write.WriteServer()
