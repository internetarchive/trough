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
    controller.start()
    controller.check_config()
    while True:
        controller.check_health()
        started = time.time()
        controller.sync()
        if not args.server:
            elapsed = time.time() - started
            gc_timeout = max(settings['SYNC_LOOP_TIMING'] - elapsed - 20, 0)
            controller.collect_garbage(timeout=gc_timeout)
        elapsed = time.time() - started
        sleep_time = max(settings['SYNC_LOOP_TIMING'] - elapsed, 0)
        logging.info('Sleeping for %.1f seconds' % sleep_time)
        time.sleep(sleep_time)
