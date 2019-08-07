import trough
from trough.settings import settings, init_worker

trough.settings.configure_logging()

init_worker()

# setup uwsgi endpoint
application = trough.write.WriteServer()
