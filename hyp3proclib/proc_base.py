# proc_base.py
# Rohan Weeden
# Created: May 15, 2018

# Base class for general processing scripts

import time

from proc_lib import get_queue_item, setup
from proc_lib.logger import log
from proc_lib.instance_tracking import manage_instance_and_lockfile

class Processor(object):
    def __init__(self, proc_name, proc_func, sleep_time=0, force_proc=False, stop_if_none=False):
        self.proc_name = proc_name
        self.proc_func = proc_func
        self.sleep_time = sleep_time
        self.force_proc = force_proc
        self.stop_if_none = stop_if_none

    def run(self):
        self.cfg = setup(self.proc_name)
        
        with manage_instance_and_lockfile(self.cfg):
            total = self.cfg['num_to_process']

            log.info('Starting')
            log.debug('Processing {0} products.'.format(total))

            self._process_all(total)

            log.info('Done')

    def _process_all(self, total):
        for n in range(total):
            found = self._process_one(n)

            log.info('Processed {0}/{1} products.'.format(n + 1, total))

            if self.sleep_time > 0:
                time.sleep(self.sleep_time)

            if not found and self.stop_if_none:
                break

    def _process_one(self, n):
        if self.force_proc:
            self.proc_func(self.cfg, n)
        else:
            is_found = get_queue_item(self.cfg, exit=False)
            if not is_found:
                return False

            self.proc_func(self.cfg, n)

        return True
