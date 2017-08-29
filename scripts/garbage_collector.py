#!/usr/bin/env python3
import trough
from trough.settings import settings
import logging
import time

if __name__ == '__main__':
    controller = trough.sync.get_controller(False)
    controller.check_config()
    controller.collect_garbage()
