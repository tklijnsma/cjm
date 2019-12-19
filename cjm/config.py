#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cjm
import os.path as osp
import logging, os, configparser, sys
logger = logging.getLogger('cjm')

def get_cjm_dir():
    if 'CJM_DIR' in os.environ:
        return osp.abspath(os.environ['CJM_DIR'])
    else:
        return osp.expanduser('~/.cjm')

class Config(object):
    """
    Config docstring

    """

    config = None

    @classmethod
    def config_from_file(cls, conf_file=None):
        if conf_file is None:
            conf_file = osp.join(cjm.CJM_DIR, 'config')
            if not osp.isfile(conf_file):
                raise OSError('No config file found: {0}'.format(conf_file))
        logger.info('Using config file %s', conf_file)
        config = configparser.ConfigParser()
        config.read(conf_file)
        cls.config = config

    def __init__(self, section_name=None):
        """
        Constructor method
        """
        super(Config, self).__init__()
        if section_name is None:
            self.section_name = os.environ.get('CJM_CONF', 'default')
        else:
            self.section_name = section
        logger.info('Instantiating config for %s', self.section_name)
        if not self.section_name in self.config:
            raise ValueError(
                'No config {0} specified in the config'
                .format(self.section_name)
                )
        self.section = self.config[self.section_name]
        self.schedd_names = self.section.get('schedd_names', '').split(',')
        self.user = os.environ.get('USER', 'undefined')
        self.todofile = self.section.get(
            'todofile', osp.join(cjm.CJM_DIR, 'todo')
            )
        self.interesting_history_keys = [
            'ProcId',
            'ClusterId',
            'HoldReason',
            'HoldReasonCode',
            'HoldReasonSubCode',
            'RemoveReason',
            'ExitStatus',
            'JobStatus',
            'LastJobStatus',
            'LastRejMatchReason',
            'ExitBySignal',
            'NumJobStarts',
            'NumShadowStarts',
            'NumJobMatches',
            'JobRunCount',
            'TotalSuspensions',
            'GlobalJobId',
            'MemoryUsage',
            'RequestMemory',
            'MachineAttrName0',
            'MachineAttrMachine0',
            'MachineAttrMachine1',
            'MachineAttrMachine2',
            'LastRemoteHost',
            ]
        self.append_htcondor_paths()
        self.init_condor_calls()

    def append_htcondor_paths(self):
        if sys.version_info[0] == 3 and 'htcondor_paths_py3' in self.section:
            paths = self.section['htcondor_paths_py3'].split(',')
        elif sys.version_info[0] == 2 and 'htcondor_paths_py2' in self.section:
            paths = self.section['htcondor_paths_py2'].split(',')
        elif 'htcondor_paths' in self.section:
            paths = self.section['htcondor_paths'].split(',')
        else:
            return
        logger.info('Appending %s to sys.path', paths)
        sys.path.extend(paths)


    def init_condor_calls(self):
        import htcondor
        self.collector = htcondor.Collector()
        self.schedd_ads = [ self.collector.locate(htcondor.DaemonTypes.Schedd, name) for name in self.schedd_names ]
        self.schedds = [ htcondor.Schedd(ad) for ad in self.schedd_ads ]













