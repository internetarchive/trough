import logging
import yaml
import sys
import os
import socket

def get_ip():
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("example.com", 80))
    output = s.getsockname()[0]
    s.close()
    return output

def get_storage_in_bytes(settings):
    '''
    Set a reasonable default for storage quota.

    Look up the settings['LOCAL_DATA'] directory, calculate the bytes on the device on which it is mounted, take 80% of total.
    '''
    statvfs = os.statvfs(settings['LOCAL_DATA'])
    return statvfs.f_frsize * statvfs.f_blocks * 0.8

logging.basicConfig(
        stream=sys.stderr, level=logging.INFO, # snakebite raises exceptions on DEBUG
        format='%(asctime)s %(process)d %(levelname)s %(threadName)s '
               '%(name)s.%(funcName)s(%(filename)s:%(lineno)d) %(message)s')

settings = {
    'LOCAL_DATA': '/var/tmp/trough',
    'READ_THREADS': '10',
    'WRITE_THREADS': '5',
    'ELECTION_CYCLE': 10, # how frequently should I hold an election for sync master server? In seconds
    # 'ROLE': 'READ', # READ, WRITE, SYNCHRONIZE, CONSUL # commented: might not need this, handle via ansible/docker?
    'HDFS_PATH': None, # /ait/prod/trough/
    'HDFS_HOST': None,
    'HDFS_PORT': None,
    'SYNC_PORT': 6006,
    'READ_PORT': 6004,
    'WRITE_PORT': 6002,
    'EXTERNAL_IP': get_ip(),
    'HOST_CHECK_WAIT_PERIOD': 5, # if the sync master starts before anything else, poll for hosts to assign to every N seconds.
    'STORAGE_IN_BYTES': None, # this will be set later, if it is not set in settings.yml
    'HOSTNAME': socket.gethostname(),
    'READ_NODE_DNS_TTL': 60 * 10, # 10 minute default
    'READ_DATABASE_DNS_TTL': 60 * 10, # 10 minute default
    'SYNC_LOOP_TIMING': 60 * 2, # do a 'sync' loop every N seconds (default: 2m. applies to both local and master sync nodes)
}

try:
    with open('/etc/trough/settings.yml') as f:
        yaml_settings = yaml.load(f)
        for key in yaml_settings.keys():
            settings[key] = yaml_settings[key]
        if settings['STORAGE_IN_BYTES'] is None:
            settings['STORAGE_IN_BYTES'] = get_storage_in_bytes(settings)
except (IOError, AttributeError) as e:
    logging.warn('%s -- using default settings', e)
    
