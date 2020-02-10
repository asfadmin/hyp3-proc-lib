# config.py
# Rohan Weeden
# May 16, 2018

# Module for proc_lib config functions

from six.moves.configparser import ConfigParser
import os

from .logger import log

config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, "config"))
config = ConfigParser()
config.optionxform = str
config_file = os.path.join(config_path, 'proc' + '.cfg')
config.read(config_file)


def is_config(section, key):
    if config.has_section(section) and config.has_option(section, key):
        return is_yes(config.get(section, key))
    return False


def is_yes(s):
    if s is None:
        return False
    return s == "1" or s.upper() == "YES" or s.upper() == "TRUE"


def get_config(section, key, default=None):
    if config.has_section(section) and config.has_option(section, key):
        return config.get(section, key)
    else:
        if default is None:
            log.error('No config value for: ' + section + '/' + key)
        return default

def load_all_general_config(cfg):
    for k in dict(config.items('general')):
        cfg[k] = config.get('general',k)
