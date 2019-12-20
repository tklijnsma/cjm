#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cjm
import os.path as osp
import logging, os, configparser, sys
logger = logging.getLogger('cjm')

class ConfigCollection(object):
    """
    Reads a config file with all the different configuration options
    """
    def __init__(self, config_file):
        """
        Constructor method
        """
        super(ConfigCollection, self).__init__()
        self.config_file = config_file
        if not osp.isfile(config_file):
            raise OSError(
                'Tried to obtain configuration for cjm from {0}, '
                'but no such file exists.'
                .format(self.config_file)
                )
        logger.info('Using config file %s', self.config_file)
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

    def sections(self):
        return [ k for k in self.config.keys() if not k == 'DEFAULT' ]

    def get_config(self, config_name):
        if config_name is None:
            config_name = self.sections()[0]
            logger.warning('No specific config specified, picking configuration %s', config_name)
        if not config_name in self.sections():
            raise ValueError(
                'No configuration {0} found in {1}'
                .format(config_name, self.config_file)
                )
        return Config(config_name, self.config[config_name])


class Config(object):
    """
    Configuration object to be consumed by the todo classes.
    Should instantiated via ConfigCollection.get_config.
    """
    def __init__(self, name, section):
        """
        Constructor method
        """
        super(Config, self).__init__()
        self.name = name
        self.section = section

        if 'schedd_names' in self.section:
            self.schedd_names = [ str(s) for s in self.section['schedd_names'].split(',') ]
            logger.info('Available schedd_names are %s', self.schedd_names)
        else:
            logger.warning(
                'No schedulers implemented for configuration %s '
                '(define schedd_names in the config file)',
                self.name
                )

        self.user = os.environ.get('USER', 'undefined')

        if 'todofile' in self.section:
            self.set_todofile(self.section['todofile'])
        elif not cjm.CJM_TODO_FILE is None:
            self.set_todofile(cjm.CJM_TODO_FILE)
        else:
            self.set_todofile(osp.join(cjm.CJM_DIR, 'todo'))

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
        # Verbose import of htcondor, to crash early if it doesn't exist
        # This should be the first time htcondor is attempted to be imported
        if 'htcondor' in sys.modules:
            logger.info('Found htcondor: %s', sys.modules['htcondor'])
        else:
            logger.info('Will try to import htcondor')
        import htcondor
        logger.debug('Loaded htcondor module in cjm.config: %s', htcondor)
        self.collector = htcondor.Collector()
        self.schedd_ads = [ self.collector.locate(htcondor.DaemonTypes.Schedd, name) for name in self.schedd_names ]
        self.schedds = [ htcondor.Schedd(ad) for ad in self.schedd_ads ]

    def set_todofile(self, todofile):
        self.todofile = todofile
        logger.debug('Todo file for this config is set to %s', self.todofile)

