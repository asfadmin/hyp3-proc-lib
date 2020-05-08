"""Module for all proc_lib logging functions"""

from __future__ import print_function, absolute_import, division, unicode_literals

import logging
import os
import sys
import time

import hyp3proclib

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
        log_file = os.path.join(hyp3proclib.default_log_dir, cfg['proc_name'] + '.log')
        handler_file = logging.FileHandler(filename=log_file)
        handler_file.setFormatter(formatter)
        log.addHandler(handler_file)

    handler_stream = logging.StreamHandler(sys.stdout)
    handler_stream.setFormatter(formatter)
    log.addHandler(handler_stream)
