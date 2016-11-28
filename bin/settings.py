import logging
import yaml
import sys

logging.basicConfig(
        stream=sys.stderr, level=logging.INFO,
        format='%(asctime)s %(process)d %(levelname)s %(threadName)s '
               '%(name)s.%(funcName)s(%(filename)s:%(lineno)d) %(message)s')

settings = {
    'DATA_ROOT': '/var/tmp/trough',
    'READ_THREADS': '10',
    'WRITE_THREADS': '5',
    'HDFS_SOURCE_PATTERN': '/ait/prod/trough/*/*.sqlite',
    'ROLE': 'READ', # READ, WRITE, SYNCHRONIZE, CONSUL
}

try:
    with open('conf/settings.yml') as f:
        yaml_settings = yaml.load(f)
        for key in yaml_settings.keys():
            settings[key] = yaml_settings[key]
except (IOError, AttributeError) as e:
    logging.warn('%s -- using default settings', e)
    
