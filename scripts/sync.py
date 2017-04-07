#!/usr/bin/env python3
import trough
from trough.settings import settings
import logging
import time

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Run a "server" sync process, which controls other sync processes, ' \
        'or a "local" sync process, which loads segments onto the current machine and performs health checks.')

    parser.add_argument('--server', dest='server', action='store_true',
                        help='run in server mode: control the actions of other local synchronizers.')
    args = parser.parse_args()

    controller = trough.sync.get_controller(args.server)
    controller.check_config()
    while True:
        controller.sync()
        logging.info('Sleeping for %s seconds' % settings['SYNC_LOOP_TIMING'])
        time.sleep(settings['SYNC_LOOP_TIMING'])