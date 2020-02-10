# logging.py
# Rohan Weeden
# Created: May 16, 2018

# Module for all proc_lib logging functions

import logging
import os
import sys
import time


log = logging.getLogger(__file__)


def setup_logger(cfg, verbose):

    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    formatter.converter = time.gmtime

    if verbose:
        lvl = logging.DEBUG
    else:
        lvl = logging.INFO

    log.setLevel(lvl)

    if cfg['write_log_file'] is True:
        log_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, "log"))
        log_file = os.path.join(log_path, cfg['proc_name'] + '.log')
        handler_file = logging.FileHandler(filename=log_file)
        handler_file.setFormatter(formatter)
        log.addHandler(handler_file)

    handler_stream = logging.StreamHandler(sys.stdout)
    handler_stream.setFormatter(formatter)
    log.addHandler(handler_stream)
