#!/usr/bin/env python3
import trough
from trough.settings import settings
import logging
import time
import datetime

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Run a "server" sync process, which controls other sync processes, ' \
        'or a "local" sync process, which loads segments onto the current machine and performs health checks.')

    parser.add_argument('--server', dest='server', action='store_true',
                        help='run in server mode: control the actions of other local synchronizers.')
    args = parser.parse_args()

    controller = trough.sync.get_controller(args.server)
    controller.start()
    controller.check_config()
    while True:
        started = datetime.datetime.now()
        controller.sync()
        loop_duration = datetime.datetime.now() - started
        sleep_time = settings['SYNC_LOOP_TIMING'] - loop_duration.total_seconds()
        sleep_time = sleep_time if sleep_time > 0 else 0
        logging.info('Sleeping for %s seconds' % round(sleep_time))
        time.sleep(sleep_time)
