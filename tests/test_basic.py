from unittest import TestCase
from mock import Mock, MagicMock, patch
import logging, os, sys

# ____________________________________________________
# Mocking htcondor python bindings for dev

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

ad = FakeClassAd(
    ServerTime = long(1576279734),
    JobStatus = long(5),
    HoldReasonSubCode = long(0),
    HoldReasonCode = long(3),
    ProcId = long(1),
    ClusterId = long(63826560),
    HoldReason = "The job attribute OnExitHold expression '(ExitBySignal == true) || (ExitCode != 0)' evaluated to TRUE",
    )

htcondor = MagicMock()
htcondor.Schedd.return_value.xquery.return_value = [ad]
sys.modules['htcondor'] = htcondor
import cjm
# ____________________________________________________

class TestBasic(TestCase):
        
    def test_imported_htcondor_is_mock(self):
        self.assertIsInstance(cjm.config.htcondor, MagicMock)

    def test_mocked_schedd_returns_fake_ad(self):
        qstate = cjm.HTCondorQueueState('63826560')
        jobs = list(qstate.xquery())
        cjm.logger.info('htcondor.Schedd.xquery: %s', htcondor.Schedd.xquery)
        self.assertEqual(str(jobs[0]['ClusterId']), '63826560')

