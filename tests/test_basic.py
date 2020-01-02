from unittest import TestCase
try:
    from mock import Mock, MagicMock, patch
except ImportError:
    from unittest.mock import Mock, MagicMock, patch
import logging, os, sys, copy, tempfile, glob
import os.path as osp


# ____________________________________________________
# Mocking htcondor python bindings for dev

def setup_testlogger():
    formatter = logging.Formatter(
        fmt = '[basictestlogger|%(levelname)8s|%(asctime)s|%(module)s]: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
        )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger = logging.getLogger('testlogger')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger
logger = setup_testlogger()

tests_dir = osp.dirname(osp.abspath(__file__))
os.environ['CJM_DIR'] = tests_dir
os.environ['CJM_CONF'] = 'test'

class FakeClassAd(dict):
    """docstring for FakeClassAd"""
    def __init__(self, *args, **kwargs):
        super(FakeClassAd, self).__init__(*args, **kwargs)

    def __repr__(self):
        r = (
            [ '\n    [ <fake ad>' ]
            + ['        {0} = {1}'.format(k, self[k]) for k in self.keys() ]
            + [ '   ]' ]
            )
        return '\n'.join(r)

htcondor = MagicMock()
sys.modules['htcondor'] = htcondor
import cjm

# ____________________________________________________

class TestHTCondorMockSetup(TestCase):

    def setUp(self):
        self.todo_state_dict = {
            'cluster_id' : '63826560',
            'submission_path' : 'fake/test/path',
            'all' : '0,1',
            'idle' : '0,1',
            }
        self.ads = [
            FakeClassAd(
                ServerTime = 1576279734,
                JobStatus = 5,
                HoldReasonSubCode = 0,
                HoldReasonCode = 3,
                ProcId = 1,
                ClusterId = 63826560,
                HoldReason = "The job attribute OnExitHold expression '(ExitBySignal == true) || (ExitCode != 0)' evaluated to TRUE",
                MemoryUsage = 1000,
                RequestMemory = 2048,
                ),
            FakeClassAd(
                ServerTime = 1576279735,
                JobStatus = 2,
                HoldReasonSubCode = 0,
                HoldReasonCode = 3,
                ProcId = 0,
                ClusterId = 63826560,
                HoldReason = "",
                MemoryUsage = 1000,
                RequestMemory = 2048,
                ),
            ]
        htcondor.Schedd.return_value.xquery.return_value = self.ads
        htcondor.Schedd.return_value.history.return_value = self.ads[:1]
        self.todo_state = cjm.HTCondorTodoItem.from_section('test', self.todo_state_dict)


class TestBasic(TestHTCondorMockSetup):

    def test_imported_htcondor_is_mock(self):
        self.assertIsInstance(cjm.todo.htcondor, MagicMock)

    def test_mocked_schedd_returns_fake_ad(self):
        qstate = cjm.HTCondorQueueState('63826560')
        jobs = list(qstate.xquery())
        cjm.logger.info('htcondor.Schedd.xquery: %s', htcondor.Schedd.xquery)
        self.assertEqual(str(jobs[0]['ClusterId']), '63826560')

    def test_todo_item_reading(self):
        self.todo_state.debug_log()
        self.assertEqual(self.todo_state.all, [0, 1])

    def test_make_diff(self):
        qstate = cjm.HTCondorQueueState('63826560').read()
        diff = cjm.HTCondorUpdater(self.todo_state, qstate)
        diff.update()

    def test_get_history(self):
        history = cjm.utils.get_job_history_htcondor(cluster_id='9999', proc_id='9', schedd=htcondor.Schedd())
        self.assertEqual(history['JobStatus'], 5)

    def test_copy_todo_item_is_shallow_for_job_instances(self):
        self.todo_state.jobs[0].testlist = ['test']
        new_todo_state = self.todo_state.copy()
        self.assertIsNot(self.todo_state.jobs, new_todo_state.jobs)
        self.assertIs(self.todo_state.jobs[0], new_todo_state.jobs[0])
        self.assertIs(self.todo_state.jobs[0].testlist, new_todo_state.jobs[0].testlist)

    def test_move_job(self):
        job = self.todo_state.jobs[0]
        self.todo_state.move(job, 'failed')
        self.assertIs(job, self.todo_state.get_jobs_in_state('failed')[0])


class TestDiff(TestHTCondorMockSetup):

    def get_basic_diff(self):
        qstate = cjm.HTCondorQueueState('63826560').read()
        diff = cjm.HTCondorUpdater(self.todo_state, qstate)
        return qstate, diff

    def test_make_diff(self):
        qstate, diff = self.get_basic_diff()
        diff.update()

    def test_makes_permanent_failure_for_removed_status(self):
        self.ads[0]['JobStatus'] = 3
        qstate, diff = self.get_basic_diff()
        new_todo_state = diff.update()
        self.assertEqual(new_todo_state.get_jobs_in_state('failed')[0].proc_id, self.ads[0].proc_id)

    def test_resubmit_for_memory_exceeding(self):
        ad = self.ads[0]
        ad['HoldReasonCode'] = 34
        ad['MemoryUsage'] = 2100
        qstate, diff = self.get_basic_diff()
        new_todo_state = diff.update()
        htcondor.Schedd.return_value.edit.assert_called_with(
            '{0}.{1}'.format(ad['ClusterId'], ad['ProcId']),
            'RequestMemory',
            int(2*ad['RequestMemory'])
            )

    def test_becomes_done_for_unlisted_exitcode_zero(self):
        ad = self.ads[1]
        del self.ads[1]
        self.ads[0]['ExitCode'] = 0 # history is mocked to return first ad, this is hacky
        qstate, diff = self.get_basic_diff()
        new_todo_state = diff.update()
        self.assertEqual(new_todo_state.get_jobs_in_state('done')[0].proc_id, ad['ProcId'])

    def test_becomes_failed_for_unlisted_exitcode_nonzero(self):
        ad = self.ads[1]
        del self.ads[1]
        self.ads[0]['ExitCode'] = 9 # history is mocked to return first ad, this is hacky
        qstate, diff = self.get_basic_diff()
        new_todo_state = diff.update()
        self.assertEqual(new_todo_state.get_jobs_in_state('failed')[0].proc_id, ad['ProcId'])

    def test_is_finished(self):
        del self.todo_state_dict['idle']
        self.todo_state_dict['done'] = '0'
        self.todo_state_dict['failed'] = '1'
        self.todo_state = cjm.HTCondorTodoItem.from_section('test', self.todo_state_dict)
        self.todo_state.debug_log()
        self.assertTrue(self.todo_state.is_finished()['finished'])

    def test_is_finished_after_update(self):
        del self.todo_state_dict['idle']
        self.todo_state_dict['done'] = '0'
        self.todo_state_dict['failed'] = '1'
        self.todo_state = cjm.HTCondorTodoItem.from_section('test', self.todo_state_dict)
        htcondor.Schedd.return_value.xquery.return_value = []
        qstate, diff = self.get_basic_diff()
        new_todo_state = diff.update()
        self.assertTrue(new_todo_state.is_finished()['finished'])

class TestUtils(TestCase):

    def test_tail(self):
        fd, path = tempfile.mkstemp()
        logger.info('Opened tmpfile %s', path)
        try:
            with os.fdopen(fd, 'w') as tmp:
                # do stuff with temp file
                for i in range(10,0,-1):
                    tmp.write('line{0}\n'.format(i))
            lines = cjm.utils.tail(path, 3)
            expected_lines = ['line3', 'line2', 'line1']
            self.assertEqual(lines, expected_lines)
        finally:
            os.remove(path)

    def test_submit(self):
        try:
            _bu_run_command = cjm.utils.run_command
            cjm.utils.run_command = MagicMock()
            cjm.utils.run_command.return_value = [
                'Querying the CMS LPC pool and trying to find an available schedd...',
                '',
                'Attempting to submit jobs to lpcschedd2.fnal.gov',
                '',
                'Submitting job(s).....',
                '5 job(s) submitted to cluster 34236250.',
                ]
            cluster_id, n_jobs, output = cjm.utils.submit(['some command line'])
            self.assertEqual(cluster_id, 34236250)
            self.assertEqual(n_jobs, 5)
        finally:
            cjm.utils.run_command = _bu_run_command


class TestRotatingFileHandler(TestCase):

    filename = osp.join(osp.dirname(__file__), 'rotatingtest.log')

    def tearDown(self):
        for f in self.get_files():
            os.remove(f)

    def get_files(self):
        return glob.glob(self.filename + '*')

    def count(self):
        return len(self.get_files())

    def logfile_contains(self, logfile, text):
        if not osp.isfile(logfile):
            logger.error('%s does not exist', logfile)
            return False
        with open(logfile) as f:
            contents = f.read()
        return text in contents

    def test_basic_rotating_file_handler(self):
        self.assertEqual(self.count(), 0)
        handler = cjm.RotatingFileHandler(self.filename)
        self.assertEqual(self.count(), 1)

        logger = logging.getLogger('tmp')
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)

        logger.info('####test1####')
        self.assertTrue(self.logfile_contains(self.filename, '####test1####'))

        handler.perform_rotation()
        self.assertEqual(self.count(), 2)
        self.assertTrue(self.logfile_contains(self.filename + '.1', '####test1####'))

        logger.info('####test2####')
        self.assertTrue(self.logfile_contains(self.filename, '####test2####'))

    def test_adding_rotating_file_handler(self):
        self.assertEqual(self.count(), 0)
        handler = cjm.add_rotating_file_handler(self.filename)
        self.assertEqual(self.count(), 1)

        cjm.logger.info('####test1####')
        self.assertTrue(self.logfile_contains(self.filename, '####test1####'))

        handler.perform_rotation()
        self.assertEqual(self.count(), 2)
        self.assertTrue(self.logfile_contains(self.filename + '.1', '####test1####'))

        cjm.logger.info('####test2####')
        self.assertTrue(self.logfile_contains(self.filename, '####test2####'))

