#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cjm
import os.path as osp
import logging
logger = logging.getLogger('cjm')

class Cluster(object):
    """
    Cluster docstring

    :param id: ID of the condor cluster
    :type id: int
    """
    def __init__(self, id='all', given_process_ids=None, config=None):
        """
        Constructor method
        """
        super(Cluster, self).__init__()
        self.config = cjm.CONFIG if config is None else config
        self.id = id
        self.given_process_ids = given_process_ids
        # The ClassAd attributes that will be queried for
        self.projection = [
            'ClusterId',
            'ProcId',
            'JobStatus',
            'HoldReasonCode',
            'HoldReasonSubCode'
            ]
        self.requirements = 'Owner=="{0}"'.format(self.config.user)
        if not self.id == 'all':
            self.requirements += '&& ClusterId=={0}'.format(self.id)

    def xquery(self, projection=None, requirements=None):
        if projection is None: projection = self.projection
        if requirements is None: requirements = self.requirements
        jobs = []
        for schedd in self.config.schedds:
            for job in schedd.xquery(
                requirements=self.requirements,
                projection=self.projection
                ):
                job.schedd = schedd  # append manually the scheduler the job belonged to
                yield job

    def jobs(self, *args, **kwargs):
        return list(self.xquery(*args, **kwargs))

    # def todoline(self):
    #     if self.given_process_ids is None:
    #         process_ids = '?'
    #     else:
    #         process_ids = ','.join([ str(i) for i in self.given_process_ids ])
    #     todoline = '{0} {1}'.format(
    #         self.id, process_ids
    #         )
    #     return todoline

    # def compare(self):
    #     jobs = self.jobs()






