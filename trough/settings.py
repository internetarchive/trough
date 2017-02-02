import logging
import yaml
import sys
import os
import socket

class Settings(dict):
    def __init__(self, settings_path=None):
        self.settings_path = settings_path
        self._settings = {
            'LOCAL_DATA': '/var/tmp/trough',
            'READ_THREADS': '10',
            'WRITE_THREADS': '5',
            'ELECTION_CYCLE': 10, # how frequently should I hold an election for sync master server? In seconds
            # 'ROLE': 'READ', # READ, WRITE, SYNCHRONIZE, CONSUL # commented: might not need this, handle via ansible/docker?
            'HDFS_PATH': None, # /ait/prod/trough/
            'HDFS_HOST': None,
            'HDFS_PORT': None,
            'READ_PORT': 6004,
            'WRITE_PORT': 6002,
            'SYNC_PORT': 6001,
            'EXTERNAL_IP': self.get_ip(),
            'HOST_CHECK_WAIT_PERIOD': 5, # if the sync master starts before anything else, poll for hosts to assign to every N seconds.
            'STORAGE_IN_BYTES': None, # this will be set later, if it is not set in settings.yml
            'HOSTNAME': socket.gethostname(),
            'READ_NODE_DNS_TTL': 60 * 10, # 10 minute default
            'READ_DATABASE_DNS_TTL': 60 * 10, # 10 minute default
            'SYNC_LOOP_TIMING': 60 * 2, # do a 'sync' loop every N seconds (default: 2m. applies to both local and master sync nodes)
            'CONSUL_ADDRESS': '127.0.0.1', # default to 'localhost' for consul connection.
            'CONSUL_PORT': 8500, # default to port 8500 (default) for consul
            'MINIMUM_ASSIGNMENTS': 2,
            'LOG_LEVEL': 'INFO',
            'SEGMENT_INITIALIZATION_SQL': os.path.join(os.path.dirname(__file__), os.path.pardir(), "conf", "intialize.sql")
        }
        logging.basicConfig(
            stream=sys.stderr, level=logging.ERROR, # snakebite raises exceptions on DEBUG
            format='%(asctime)s %(process)d %(levelname)s %(threadName)s '
                   '%(name)s.%(funcName)s(%(filename)s:%(lineno)d) %(message)s')
        try:
            with open(self.settings_path if self.settings_path else '/etc/trough/settings.yml') as f:
                yaml_settings = yaml.load(f)
                for key in yaml_settings.keys():
                    self._settings[key] = yaml_settings[key]
        except (IOError, AttributeError) as e:
            logging.warning('%s -- using default self._settings', e)

        # if the user provided a lambda, we have to eval() it, :gulp:
        if "lambda" in str(self._settings['MINIMUM_ASSIGNMENTS']):
            self._settings['MINIMUM_ASSIGNMENTS'] = eval(self._settings['MINIMUM_ASSIGNMENTS'])

        if not os.path.isdir(self._settings['LOCAL_DATA']):
            logging.warning("LOCAL_DATA path %s does not exist. Attempting to make dirs." % self._settings['LOCAL_DATA'])
            os.makedirs(self._settings['LOCAL_DATA'])

        if self._settings['STORAGE_IN_BYTES'] is None:
            storage_in_bytes = self.get_storage_in_bytes()
            logging.warning("STORAGE_IN_BYTES is not set. Setting to 80%% of storage on volume containing %s (LOCAL_DATA): %s bytes" % (self._settings['LOCAL_DATA'], storage_in_bytes))
            self._settings['STORAGE_IN_BYTES'] = storage_in_bytes

        log_level = self._settings.get('LOG_LEVEL', 'ERROR')
        logging.basicConfig(
            stream=sys.stderr, level=getattr(logging, log_level), # snakebite raises exceptions on DEBUG
            format='%(asctime)s %(process)d %(levelname)s %(threadName)s '
                   '%(name)s.%(funcName)s(%(filename)s:%(lineno)d) %(message)s')

    def get_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("example.com", 80))
        output = s.getsockname()[0]
        s.close()
        return output

    def get_storage_in_bytes(self):
        '''
        Set a reasonable default for storage quota.

        Look up the settings['LOCAL_DATA'] directory, calculate the bytes on the device on which it is mounted, take 80% of total.
        '''
        statvfs = os.statvfs(self._settings['LOCAL_DATA'])
        return int(statvfs.f_frsize * statvfs.f_blocks * 0.8)


    def __getitem__(self, key):
        return self._settings[key]

    def get(self, key, default=None):
        return self._settings.get(key, default)

