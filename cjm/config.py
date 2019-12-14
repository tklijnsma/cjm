#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cjm
import os.path as osp
import logging, os
logger = logging.getLogger('cjm')

import htcondor
logger.debug('Loaded htcondor module in cjm.config: %s', htcondor)

class Config(object):
    """
    Config docstring

    """
    def __init__(self, schedd_names=None):
        """
        Constructor method
        """
        super(Config, self).__init__()
        logger.info('Instantiating config')
        self.schedd_names = cjm.SCHEDD_NAMES if schedd_names is None else schedd_names
        self.user = os.environ['USER']
        self.todofile = osp.expanduser('~/.cjm/todo')
        self.init_condor_calls()

    def init_condor_calls(self):
        self.collector = htcondor.Collector()
        self.schedd_ads = [ self.collector.locate(htcondor.DaemonTypes.Schedd, name) for name in self.schedd_names ]
        self.schedds = [ htcondor.Schedd(ad) for ad in self.schedd_ads ]
