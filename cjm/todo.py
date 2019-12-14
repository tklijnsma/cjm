#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cjm
import os.path as osp
import logging
import configparser
import pprint
from time import strftime
logger = logging.getLogger('cjm')

class TodoList(object):
    """
    TodoList docstring

    """
    def __init__(self, todofile=None):
        """
        Constructor method
        """
        super(TodoList, self).__init__()
        self.todofile = cjm.CONFIG.todofile if todofile is None else todofile
        self.todo = configparser.ConfigParser()
        self.todo.read(self.todofile)

    def write(self):
        logger.info('Overwriting %s', self.todofile)
        with open(self.todofile, 'w') as f:
            config.write(f)

    def push(self, key, dictlike):
        self.todo[key] = dictlike

    def pop(self, key):
        del self.todo[key]



class TodoItem(object):
    """docstring for TodoItem"""
    def __init__(self, sectionname, section):
        super(TodoItem, self).__init__()
        self.section = section
        self.key = sectionname
        # Mandatory keys
        self.cluster_id = section['cluster_id']
        self.submission_path = section['submission_path']
        self.all = section['all'].split(',')
        # Keys with a default
        self.monitor_level = section.get('monitor_level', 'high')
        self.submission_time = section.get('submission_time', None)
        self.schedd_name = section.get('schedd_name', None)
        # Fill the state attributes
        self.states = [
            'idle',
            'running',
            'removed',
            'completed',
            'held',
            'transfering',
            'suspended',
            'done',
            'failed',
            ]
        self.idle = self.section['idle'].split(',')
        self.running = self.section['running'].split(',')
        self.removed = self.section['removed'].split(',')
        self.completed = self.section['completed'].split(',')
        self.held = self.section['held'].split(',')
        self.transfering = self.section['transfering'].split(',')
        self.suspended = self.section['suspended'].split(',')
        self.done = self.section['done'].split(',')
        self.failed = self.section['failed'].split(',')
        self.failurecounts = { pair.split(':')[0] : pair.split(':')[1] for pair in self.section['failurecounts'].split(',') }
        self.state_per_job = {}

    def log_state(self):
        logger.debug('State of %s', self.key)
        logger.debug('idle: %s', self.idle)
        logger.debug('running: %s', self.running)
        logger.debug('removed: %s', self.removed)
        logger.debug('completed: %s', self.completed)
        logger.debug('held: %s', self.held)
        logger.debug('transfering: %s', self.transfering)
        logger.debug('suspended: %s', self.suspended)
        logger.debug('done: %s', self.done)
        logger.debug('failed: %s', self.failed)
        logger.debug('failurecounts: %s', self.failurecounts)
        
    def get_state_per_job(self):
        for state in self.states:
            proc_ids = getattr(self, state)
            for i in proc_ids:
                self.state_per_job[i] = state
        logger.debug('Retrieved state per job: %s', self.state_per_job)



class HTCondorQueueState(object):
    """docstring for HTCondorQueueState"""
    def __init__(self, cluster_id, config=None):
        super(HTCondorQueueState, self).__init__()
        self.config = cjm.CONFIG if config is None else config
        self.cluster_id = cluster_id
        # variables to get from job classad
        self.projection = [
            'ClusterId',
            'ProcId',
            'JobStatus',
            'HoldReason',
            'HoldReasonCode',
            'HoldReasonSubCode'
            ]
        self.requirements = (
            'Owner=="{0}" '
            '&& ClusterId=={1} '
            .format(self.config.user, self.cluster_id)
            )

        self.state_per_job = {}
        self.listed_jobs = []
        self.jobs = []
        self.jobs_by_id = {}

    def pformat(self):
        return self.__repr__()[:-1] + ' state_per_job: ' + pprint.pformat(self.state_per_job) + ' >'

    def xquery(self, projection=None, requirements=None, schedd=None):
        if projection is None: projection = self.projection
        if requirements is None: requirements = self.requirements
        jobs = []
        # If the exact scheduler is known, just use it, but otherwise query all
        schedds = self.config.schedds if schedd is None else [schedd]
        for schedd in schedds:
            logger.debug('Querying %s, xquery: %s', schedd, schedd.xquery)
            for job in schedd.xquery(
                requirements=self.requirements,
                projection=self.projection
                ):
                job.schedd = schedd  # append manually the scheduler the job belonged to
                logger.debug(
                    'Got %s.%s at %s from query: %s',
                    job.__class__.__module__, job.__class__.__name__, hex(id(job)), job
                    )
                yield job

    def get_state(self):
        self.jobs = list(self.xquery())
        for job in self.jobs:
            logger.debug(type(job['ServerTime']))
            logger.debug(type(job['JobStatus']))
            logger.debug(type(job['HoldReason']))
            logger.debug(type(job['HoldReasonSubCode']))
            logger.debug(type(job['HoldReasonCode']))
            logger.debug(type(job['ProcId']))
            logger.debug(type(job['ClusterId']))
            proc_id = str(job['ProcId']) # type is long by default, but easier to keep working with strs
            self.listed_jobs.append(proc_id)
            self.jobs_by_id[proc_id] = job
            self.state_per_job[proc_id] = int(job['JobStatus'])


# HIER VERDER
# State van zowel todo als condor_q nu in twee classes, nu de diff maken
# diff moet naar email gelogd worden (alleen voor high monitoring later)







