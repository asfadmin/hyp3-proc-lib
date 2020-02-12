"""Module for proc_lib config functions"""

from six.moves.configparser import ConfigParser

import hyp3proclib
from hyp3proclib.logger import log


def init_config(config_file=None):
    """Load proc.cfg"""
    hyp3proclib.default_cfg = ConfigParser()
    hyp3proclib.default_cfg.optionxform = str
    if config_file is None:
        config_file = hyp3proclib.default_config_file
    hyp3proclib.default_cfg.read(config_file)


def is_config(section, key, config_file=None):
    if hyp3proclib.default_cfg is None:
        log.debug('Config keys requested from uninitialized config file!')
        init_config(config_file=config_file)
    if hyp3proclib.default_cfg.has_section(section) \
            and hyp3proclib.default_cfg.has_option(section, key):
        return is_yes(hyp3proclib.default_cfg.get(section, key))
    return False


def is_yes(s):
    if s is True:
        return True
    if s is None:
        return False
    return s == "1" or s.upper() == "YES" or s.upper() == "TRUE"


def get_config(section, key, default=None, config_file=None):
    if hyp3proclib.default_cfg is None:
        log.debug('Config keys requested from uninitialized config file!')
        init_config(config_file=config_file)
    if hyp3proclib.default_cfg.has_section(section) \
            and hyp3proclib.default_cfg.has_option(section, key):
        return hyp3proclib.default_cfg.get(section, key)
    else:
        if default is None:
            log.error('No config value for: ' + section + '/' + key)
        return default


def load_all_general_config(cfg, config_file=None):
    if hyp3proclib.default_cfg is None:
        log.debug('Config keys requested from uninitialized config file!')
        init_config(config_file=config_file)
    for k in dict(hyp3proclib.default_cfg.items('general')):
        cfg[k] = hyp3proclib.default_cfg.get('general', k)
