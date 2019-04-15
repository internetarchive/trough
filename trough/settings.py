import logging
import yaml
import sys
import os
import socket

# logging.basicConfig(
#     stream=sys.stderr, level=getattr(logging, os.environ.get('TROUGH_LOG_LEVEL', 'INFO')), # snakebite raises exceptions on DEBUG
#     format='%(asctime)s %(process)d %(levelname)s %(threadName)s '
#            '%(name)s.%(funcName)s(%(filename)s:%(lineno)d) %(message)s')

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('10.255.255.255', 1)) # ip doesn't need to be reachable
    output = s.getsockname()[0]
    s.close()
    return output

def get_storage_in_bytes():
    '''
    Set a reasonable default for storage quota.

    Look up the settings['LOCAL_DATA'] directory, calculate the bytes on the
    device on which it is mounted, take 80% of total.
    '''
    path = settings['LOCAL_DATA']
    while True:
        try:
            statvfs = os.statvfs(path)
            return int(statvfs.f_frsize * statvfs.f_blocks * 0.8)
        except:
            path = os.path.dirname(path)

settings = {
    'LOCAL_DATA': '/var/tmp/trough',
    'READ_THREADS': '10',
    'WRITE_THREADS': '5',
    'ELECTION_CYCLE': 10, # how frequently should I hold an election for sync master server? In seconds
    # 'ROLE': 'READ', # READ, WRITE, SYNCHRONIZE, CONSUL # commented: might not need this, handle via ansible/docker?
    'HDFS_PATH': '/tmp/trough', # /ait/prod/trough/
    'HDFS_HOST': 'localhost',
    'HDFS_PORT': 8020,
    'READ_PORT': 6444,
    'WRITE_PORT': 6222,
    'SYNC_SERVER_PORT': 6111,
    'SYNC_LOCAL_PORT': 6112,
    'EXTERNAL_IP': None,
    'HOST_CHECK_WAIT_PERIOD': 5, # if the sync master starts before anything else, poll for hosts to assign to every N seconds.
    'STORAGE_IN_BYTES': None, # this will be set later, if it is not set in settings.yml
    'HOSTNAME': socket.gethostname(),
    'READ_NODE_DNS_TTL': 60 * 10, # 10 minute default
    'READ_DATABASE_DNS_TTL': 60 * 10, # 10 minute default
    'SYNC_LOOP_TIMING': 60 * 2, # do a 'sync' loop every N seconds (default: 2m. applies to both local and master sync nodes)
    'RETHINKDB_HOSTS': ["localhost",],
    'MINIMUM_ASSIGNMENTS': 2,
    'MAXIMUM_ASSIGNMENTS': 2,
    'SENTRY_DSN': None,
    'LOG_LEVEL': 'INFO',
    'ALLOWED_WRITE_VERBS': ['INSERT'], # allow inserts only by default. Enforces consistency. The user can optionally allow updates, create etc.
}

try:
    with open(os.environ.get('TROUGH_SETTINGS') or '/etc/trough/settings.yml') as f:
        yaml_settings = yaml.safe_load(f)
        for key in yaml_settings.keys():
            settings[key] = yaml_settings[key]
except (IOError, AttributeError) as e:
    logging.warning('%s -- using default settings', e)

# if the user provided a lambda, we have to eval() it, :gulp:
if "lambda" in str(settings['MINIMUM_ASSIGNMENTS']):
    settings['MINIMUM_ASSIGNMENTS'] = eval(settings['MINIMUM_ASSIGNMENTS'])

if settings['EXTERNAL_IP'] is None:
    settings['EXTERNAL_IP'] = get_ip()

if settings['STORAGE_IN_BYTES'] is None:
    settings['STORAGE_IN_BYTES'] = get_storage_in_bytes()

def init_worker():
    '''
    Some initial setup for worker nodes.
    '''
    if not os.path.isdir(settings['LOCAL_DATA']):
        logging.info("LOCAL_DATA path %s does not exist. Attempting to make dirs." % settings['LOCAL_DATA'])
        os.makedirs(settings['LOCAL_DATA'])

