#!/usr/bin/env python3
import trough
from trough.settings import settings
import logging
import time
import datetime
import sys

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Run a "server" sync process, which controls other sync processes, ' \
        'or a "local" sync process, which loads segments onto the current machine and performs health checks.')

    parser.add_argument('--server', dest='server', action='store_true',
                        help='run in server mode: control the actions of other local synchronizers.')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    logging.root.handlers = []
    logging.basicConfig(
            stream=sys.stdout,
            level=logging.DEBUG if args.verbose else logging.INFO, format=(
                '%(asctime)s %(levelname)s %(name)s.%(funcName)s'
                '(%(filename)s:%(lineno)d) %(message)s'))
    logging.getLogger('requests.packages.urllib3').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    logging.getLogger('snakebite').setLevel(logging.INFO)

    controller = trough.sync.get_controller(args.server)
    controller.start()
    controller.check_config()
    while True:
        controller.check_health()
        started = datetime.datetime.now()
        controller.sync()
        if not args.server:
            controller.collect_garbage()
        loop_duration = datetime.datetime.now() - started
        sleep_time = settings['SYNC_LOOP_TIMING'] - loop_duration.total_seconds()
        sleep_time = sleep_time if sleep_time > 0 else 0
        logging.info('Sleeping for %s seconds' % round(sleep_time))
        time.sleep(sleep_time)
