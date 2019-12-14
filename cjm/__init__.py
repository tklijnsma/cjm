#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, sys
from .logger import setup_logger
logger = setup_logger()

from . import utils

def setup_lpc():
    # if (sys.version_info > (3, 0)):
    #     condor_paths = ['/usr/lib64/python3.6/site-packages']
    # else:
    #     condor_paths = [
    #         '/usr/lib64/python2.6/site-packages',
    #         '/usr/lib64/python2.7/site-packages'
    #         ]        
    # for condor_path in condor_paths:
    #     if condor_path not in sys.path and os.path.isdir(condor_path):
    #         sys.path.append(condor_path)
    global SCHEDD_NAMES
    SCHEDD_NAMES = [
        'lpcschedd1.fnal.gov',
        'lpcschedd2.fnal.gov',
        'lpcschedd3.fnal.gov'
        ]

if os.environ['HOSTNAME'].startswith('cmslpc'):
    setup_lpc()
else:
    raise NotImplementedError(
        'Only LPC now.'
        )

# For now load default config upon initialization
from .config import Config
CONFIG = Config()

from .cluster import Cluster
from .todo import TodoList, HTCondorQueueState