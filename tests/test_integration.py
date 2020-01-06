#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, shutil, logging, sys, subprocess
import os.path as osp
from unittest import TestCase
from time import sleep
try:
    from mock import Mock, MagicMock, patch
except ImportError:
    from unittest.mock import Mock, MagicMock, patch

# Change the default CJM_DIR to the integration_tests dir
tests_dir = osp.dirname(osp.abspath(__file__))
os.environ['CJM_DIR'] = tests_dir
os.environ['CJM_CONF'] = 'test-integration'
import cjm
# cjm.add_file_handler(osp.join(tests_dir, 'cjm.log'))

def setup_testlogger():
    formatter = logging.Formatter(
        fmt = '[integrationtestlogger|%(levelname)8s|%(asctime)s|%(module)s]: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
        )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger = logging.getLogger('testlogger')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger
logger = setup_testlogger()


def get_jobs_in_cluster(cluster_id):
    classads = []
    for schedd in cjm.CONFIG.schedds:
        classads.extend(list(schedd.xquery(
            requirements = 'ClusterId=={0}'.format(cluster_id),
            projection = ['ProcId']
            )))
    return classads

def get_job(cluster_id, proc_id):
    classads = []
    for schedd in cjm.CONFIG.schedds:
        classads.extend(list(schedd.xquery(
            requirements = 'ClusterId=={0} && ProcId == {1}'.format(cluster_id, proc_id),
            projection = ['JobStatus']
            )))
    if len(classads) > 1:
        raise RuntimeError(
            'Number of jobs for query cluster_id = {0}, proc_id = {1} '
            'is not 1 ({2})'
            .format(cluster_id, proc_id, len(classads))
            )
    logger.info('Query returned %s', classads)
    if len(classads) == 0: return None
    return classads[0]

def count_jobs(cluster_id):
    return len(get_jobs_in_cluster(cluster_id))

def wait_for_job_to_finish(cluster_id, proc_id, n_sleep=10, continue_for_states = [1, 2]):
    status = 1
    while(status in continue_for_states):
        logger.info('Job %s.%s in status %s, waiting %s seconds', cluster_id, proc_id, status, n_sleep)
        sleep(n_sleep)
        job = get_job(cluster_id, proc_id)
        if job is None:
            logger.info('No job from query, so assuming done')
            return
        status = job['JobStatus']
    logger.info('Job %s.%s is in state %s', cluster_id, proc_id, status)
    return job


class TestIntegration(TestCase):

    def setUp(self):
        self._backdir = os.getcwd()
        os.chdir(tests_dir)
        logger.info('Changed dir to %s', tests_dir)
        self._cluster_ids_to_cleanup = []
        self.clear_todo_file()

    def tearDown(self):
        for cluster_id in self._cluster_ids_to_cleanup:
            cjm.utils.remove(cluster_id)
        os.chdir(self._backdir)
        logger.info('Changed dir back to %s', self._backdir)
        self.clear_todo_file()

    def clear_todo_file(self):
        todofile = osp.join(tests_dir, 'todo')
        open(todofile, 'w').close()

    def test_removal(self):
        cluster_id, n_jobs, output = cjm.utils.submit('cjmtestjob.jdl')
        counted_n_jobs = count_jobs(cluster_id)
        logger.info('Counted %s jobs in cluster_id %s', counted_n_jobs, cluster_id)
        self.assertEqual(n_jobs, counted_n_jobs)
        cjm.utils.remove(cluster_id)
        counted_n_jobs_after_remove = count_jobs(cluster_id)
        logger.info(
            'Counted %s jobs in cluster_id %s after removal',
            counted_n_jobs_after_remove, cluster_id
            )
        self.assertEqual(counted_n_jobs_after_remove, 0)

    def test_submission(self):
        todolist = cjm.TodoList()
        cluster_id, updated_todolist = todolist.submit('cjmtestjob.jdl')
        self._cluster_ids_to_cleanup.append(cluster_id)
        self.assertIn(str(cluster_id), updated_todolist.sections())
        del todolist, updated_todolist
        logger.info('Reading todolist again')
        reread_todolist = cjm.TodoList()
        self.assertIn(str(cluster_id), reread_todolist.sections())

    def test_basic_update(self):
        clean_todolist = cjm.TodoList()
        cluster_id, todolist = clean_todolist.submit('cjmtestjob_singlejob.jdl')
        self._cluster_ids_to_cleanup.append(cluster_id)
        updated_todolist = todolist.update()
        logger.info('Getting the todo item class')
        todoitem = updated_todolist.get_todoitem(str(cluster_id))
        self.assertEqual(len(todoitem.jobs), 1)
        self.assertIn(todoitem.get_state(0), ['idle', 'running'])

    def test_state_change_to_done(self):
        clean_todolist = cjm.TodoList()
        cluster_id, todolist = clean_todolist.submit('cjmtestjob.jdl')
        self._cluster_ids_to_cleanup.append(cluster_id)
        wait_for_job_to_finish(cluster_id, 0)

        logger.info('Opening todo list and performing update')
        updated_todolist = cjm.TodoList().update()
        logger.info('Getting the todo item class')
        todoitem = updated_todolist.get_todoitem(str(cluster_id))
        todoitem.debug_log()
        self.assertEqual(len(todoitem.jobs), 2)
        self.assertEqual(todoitem.get_state(0), 'done')
        self.assertIn(todoitem.get_state(1), ['idle', 'running'])

    def test_state_change_to_failed(self):
        clean_todolist = cjm.TodoList()
        cluster_id, todolist = clean_todolist.submit('cjmtestjob_withfailure.jdl')
        self._cluster_ids_to_cleanup.append(cluster_id)
        del todolist
        wait_for_job_to_finish(cluster_id, 0)

        logger.info('Opening todo list and performing update')
        todolist = cjm.TodoList()        
        new_todolist = todolist.update()
        self.assertEqual(len(new_todolist.get_section_titles()), 0)

