#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, sys
import os.path as osp
from .logger import setup_logger, add_file_handler, add_rotating_file_handler, RotatingFileHandler
logger = setup_logger()

if 'CJM_ROTFILEHANDLER' in os.environ:
    # Fix logging to a file before any other configuration to also save the logging
    # emitted *during* the configuration
    add_rotating_file_handler(os.environ['CJM_ROTFILEHANDLER'], delete_other_handlers=True)

from . import utils

# Default dir to save files related to the module
if 'CJM_DIR' in os.environ:
    CJM_DIR = os.environ['CJM_DIR']
else:
    CJM_DIR = osp.expanduser('~/.cjm')

# Config file path
def find_config_file():
    if 'CJM_CONF_FILE' in os.environ:
        return os.environ['CJM_CONF_FILE']
    for config_file in [
        osp.join(CJM_DIR, 'config'),
        osp.expanduser('~/.cjm/config'),
        osp.join(
            osp.dirname(osp.abspath(__file__)),
            '../data/config'
            )
        ]:
        if osp.isfile(config_file):
            return config_file
    else:
        # The shipped config file, ../data/config, should always exist
        raise OSError('Could not find any existing configuration file')
CJM_CONF_FILE = find_config_file()

# Name of the configuration in the config file to be used
CJM_CONF = None
if 'CJM_CONF' in os.environ:
    CJM_CONF = os.environ['CJM_CONF']

# Path to the todo file
CJM_TODO_FILE = None
if 'CJM_TODO_FILE' in os.environ:
    CJM_TODO_FILE = os.environ['CJM_TODO_FILE']

# Utility to load a config
from .config import ConfigCollection, Config
def reload_config(config_name):
    configcollection = ConfigCollection(CJM_CONF_FILE)
    return configcollection.get_config(config_name)

# Default config
CONFIG = reload_config(CJM_CONF)

from .cluster import Cluster
from .email import Email, EventCodes
from .todo import TodoList, HTCondorTodoItem, HTCondorQueueState, HTCondorUpdater
