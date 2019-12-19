#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, sys
from .logger import setup_logger
logger = setup_logger()

from . import utils

# Verbose import of htcondor
if 'htcondor' in sys.modules:
    logger.info('Found htcondor: %s', sys.modules['htcondor'])
else:
    logger.info('Will try to import htcondor')
import htcondor
logger.debug('Loaded htcondor module in cjm.config: %s', htcondor)

from .config import Config, get_cjm_dir
CJM_DIR = get_cjm_dir() # Sets the CJM_DIR global variable
Config.config_from_file() # Reads the config file
CONFIG = Config()

from .cluster import Cluster
from .todo import TodoList, HTCondorTodoItem, HTCondorQueueState, HTCondorUpdater