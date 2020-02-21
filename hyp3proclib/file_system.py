"""Module for proc_lib functions that manipulate the file system"""

from __future__ import print_function, absolute_import, division, unicode_literals

import errno
import os
import shutil
import sys
import uuid
import datetime
from contextlib import contextmanager

from hyp3proclib.logger import log


def mkdir_p(path):
    """
    Make parent directories as needed and no error if existing. Works like `mkdir -p`.
    """
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def setup_workdir(cfg):
    if cfg['user_workdir'] and len(cfg['workdir']) > 0:
        wd = cfg['workdir']
        log.info('Using previous working directory (will not process)')
        log.info('Directory is: ' + wd)
    else:
        s = cfg['proc_name'] + '_' + str(os.getpid()) + '_' + random_string(6)

        # Hack to avoid creating a bunch of pointless directories
        if 'notify' in s:
            return

        wd = os.path.join(cfg['workdir'], s)
        cfg['workdir'] = wd

        log.debug('Workdir is: ' + wd)
        if os.path.isdir(wd):
            log.warn('Working directory already exists!  Removing...')
            shutil.rmtree(wd)

        log.info('Creating work directory: ' + wd)
        os.mkdir(wd)

    # Some of the processes are location dependent!
    os.chdir(wd)


def random_string(string_length=4):
    return str(uuid.uuid4()).upper().replace("-", "")[0:string_length]


def cleanup_workdir(cfg):
    if 'workdir' in cfg:
        if os.path.isdir(cfg['workdir']):
            if cfg['keep']:
                log.info('Not removing working directory: ' + cfg['workdir'])
            else:
                log.info('Cleaning up working directory: ' + cfg['workdir'])
                shutil.rmtree(cfg['workdir'])
        else:
            log.warn('Could not clean the workdir, not found: ' + cfg['workdir'])

    cleanup_env(cfg)


def cleanup_env(cfg):
    if 'browse_images' in cfg:
        del cfg['browse_images']
    cfg['browse_lat_min'] = None
    cfg['browse_lat_max'] = None
    cfg['browse_lon_min'] = None
    cfg['browse_lon_max'] = None
    cfg['browse_epsg'] = None

    cfg['id'] = None
    cfg['granule'] = None

    cfg['workdir'] = cfg['original_workdir']


@contextmanager
def lockfile(cfg):
    check_lockfile(cfg)
    yield
    cleanup_lockfile(cfg)


def check_lockfile(cfg):
    lock_file = os.path.join(cfg['lock_dir'], cfg['proc_name'] + '.lock')
    cfg['lock_file'] = lock_file

    if os.path.isfile(lock_file):
        log.info('Lock file exists: ' + lock_file)
        log.info('Exiting -- already running.')
        sys.exit(0)

    # We use os.open with O_CREAT so that two ingests don't both do the
    # above check, and pass, and then both try to create the lock file,
    # and both succeed - this way one will fail
    try:
        o = os.open(lock_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
        fd = os.fdopen(o, 'w')
    except Exception as e:
        log.warning('Failed to open lock file: ' + str(e))
        fd = None

    if not fd:
        log.error('Could not open lock file: ' + lock_file)
        sys.exit(1)

    pid = str(os.getpid())
    fd.write(pid)
    fd.close()

    # Now check the file just in case...
    with open(lock_file, 'r') as fd:
        s = fd.read()

    if s != pid:
        log.error('Failed to correctly initialize lock file')
        sys.exit(1)
    else:
        log.info('Acquired lock file, PID is ' + pid)


def cleanup_lockfile(cfg):
    if 'lock_file' not in cfg:
        log.warn('No lock_file set!')
        return

    lock_file = cfg['lock_file']

    if 'lock_file' not in cfg or lock_file is None or len(lock_file) == 0:
        log.info('Internal error: no lock file set.')
        return

    if os.path.isfile(lock_file):
        log.info('Removing lock file ' + lock_file)
        os.unlink(lock_file)
    else:
        log.warn('Lock file not found: ' + lock_file)


def check_stop(cfg):
    check_lockfile_exists(cfg['lock_file'])

    stopfile = os.path.join(os.path.dirname(cfg['lock_file']), 'stop')
    check_stopfile(cfg, stopfile)

    check_lockfile_pid(cfg['lock_file'])


def check_lockfile_exists(lock_file):
    if not os.path.isfile(lock_file):
        log.info('Lock file does not exist')
        log.info('Stopping')
        sys.exit(0)


def check_stopfile(cfg, stopfile):
    if os.path.isfile(stopfile):
        log.info('Found stopfile: ' + stopfile)
        log.debug('Removing stopfile: ' + stopfile)
        os.remove(stopfile)
        log.info('Stopping')
        cleanup_lockfile(cfg)
        sys.exit(0)


def check_lockfile_pid(lock_file):
    pid = str(os.getpid())
    with open(lock_file, 'r') as lock:
        lock_file_pid = lock.read()
        if str(lock_file_pid) != str(pid):
            log.info('Lock file does not contain process PID')
            log.info('Process PID: "{}"   File PID: "{}"'.format(pid, lock_file_pid))
            log.info('Exiting without cleaning')
            sys.exit(0)


def add_citation(cfg, dir_):

    if not cfg['granule'].startswith('S1'):
        return

    y = int(datetime.datetime.now().year)
    ay = None
    for subdir, dirs, files in os.walk(dir_):
        for f in files:
            try:
                for item in f.split("_"):
                    if item[0:8].isdigit() and item[8] == "T" and item[9:15].isdigit():
                        ay = item[0:4]
                        break
            except:
                log.error("ERROR: Unable to determine acquisition year from filename {f}".format(f=f))
            if ay:
                break
        if ay:
            break

    if ay is None:
        ay = y

    with open(os.path.join(dir_, 'ESA_citation.txt'), 'w') as f:
        f.write('ASF DAAC {0}, contains modified Copernicus Sentinel data {1}, processed by ESA.'.format(y,ay))


def find_rtc_zip(dir_, orbit):
    log.debug('Orbit: ' + orbit)
    rtc_zip = find_in_dir(dir_, [".zip", "AP_", orbit])
    log.info("Found RTC zip: " + rtc_zip)
    return rtc_zip


def find_in_dir(dir_, all_strs, any_strs=("",)):
    if not os.path.isdir(dir_):
        return None

    for subdir, dirs, files in os.walk(dir_):
        for file in files:
            filepath = os.path.join(subdir, file)
            if any(s in filepath for s in any_strs) and all(s in filepath for s in all_strs):
                log.info('Found: ' + filepath)
                return filepath

    return None
