import trough
from trough.settings import settings
settings.init_worker()

# setup uwsgi endpoint
application = trough.write.WriteServer()
