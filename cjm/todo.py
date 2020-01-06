#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cjm
import os.path as osp
import logging, configparser, pprint, copy, os
from time import strftime
logger = logging.getLogger('cjm')
import htcondor

# Patch configparser.ConfigParser deepcopy method
def configparser_deepcopy(self):
    try:
        from StringIO import StringIO
    except ImportError:
        from io import StringIO
    # Create a deep copy of the configuration object
    config_string = StringIO()
    self.write(config_string)
    # Reset the buffer to make it ready for reading.
    config_string.seek(0)        
    new_config = ConfigParser.ConfigParser()
    new_config.readfp(config_string)
configparser.ConfigParser.__deepcopy__ = configparser_deepcopy


class TodoList(object):
    """
    TodoList docstring
    """

    def __init__(self, todofile=None, _dict=None):
        """
        Constructor method
        """
        super(TodoList, self).__init__()
        self.todofile = cjm.CONFIG.todofile if todofile is None else todofile
        self.todo = configparser.ConfigParser()
        if _dict:
            self.todo.read_dict(_dict)
            logger.info(
                'Initializing TodoList using dict %s; sections detected: %s',
                _dict, self.get_section_titles()
                )
        elif self.todofile:
            self.read()

    def read(self):
        if osp.isfile(self.todofile):
            self.todo.read(self.todofile)
            logger.info(
                'Initializing todo list using file %s; sections detected: %s',
                self.todofile, self.get_section_titles()
                )
        else:
            logger.info('Todo file %s does not exist, keeping empty configparser', self.todofile)

    def write(self, config=None):
        if config is None: config = self.todo
        logger.info('Overwriting %s', self.todofile)
        dirname = osp.dirname(self.todofile)
        if not osp.isdir(dirname):
            logger.info('Creating directory %s', dirname)
            os.makedirs(dirname)
        with open(self.todofile, 'w') as f:
            config.write(f)
        logger.debug('Wrote the following to %s:\n%s', self.todofile, self.read_plain())

    def read_plain(self):
        """
        Reads and returns the contents of the todo file as plain text.
        """
        with open(self.todofile, 'r') as f:
            contents = f.read()
        return contents

    def push(self, key, dictlike):
        self.todo[key] = dictlike

    def pop(self, key):
        del self.todo[key]

    def get_section_titles(self):
        return [ k for k in self.todo.keys() if not k == 'DEFAULT' ]

    def get_todoitem(self, cluster_id):
        """
        Returns an HTCondorTodoItem instance for cluster `cluster_id`
        """
        return HTCondorTodoItem.from_section(cluster_id, self.todo[cluster_id])

    def get_queuestate(self, cluster_id):
        """
        Returns an HTCondorQueueState instance for cluster `cluster_id`
        """
        return HTCondorQueueState(cluster_id).read()

    def update(self):
        """
        Reads the queue using the htcondor bindings, and makes an updated todolist.
        Returns an updated TodoList instance.
        Does not modify `self`, only the physical todofile.
        Writes the updated todolist automatically to the todofile.
        """
        logger.debug('Begin updating, section titles = %s', self.get_section_titles())
        new_todo = configparser.ConfigParser()
        # Instantiates an email class, which will be filled with noteworthy events
        email = cjm.Email()
        for section_title in self.get_section_titles():
            # Key `cluster_id` is expected to exist in the section
            cluster_id = self.todo[section_title]['cluster_id']
            todoitem = self.get_todoitem(cluster_id)
            queuestate = self.get_queuestate(cluster_id)
            new_todoitem = HTCondorUpdater(todoitem, queuestate, email=email).update()
            status = new_todoitem.is_finished()
            if status['finished']:
                logger.info('Finished, not parsing todo item to next update')
            else:
                new_todo[cluster_id] = new_todoitem.parse_todoitem()
        email.send_email()
        self.write(new_todo)
        return TodoList(self.todofile)

    def submit(self, command_line, monitor_level='high'):
        """
        Submits jobs according to the command line, and pushes to this todo file.
        Returns the key (in this case simply the `cluster_id`) and the updated todolist.
        Does not modify `self`, only the physical todofile.

        :param command_line: the command line that would normally be submitted to condor_submit
        :type command_line: list
        """
        new_todo = configparser.ConfigParser()
        cluster_id, n_jobs, output = cjm.utils.submit(command_line)
        new_item = {
            'cluster_id' : str(cluster_id),
            'submission_time' : strftime('%Y-%m-%d %H:%M:%S'),
            'submission_path' : os.getcwd(),
            'monitor_level' : monitor_level,
            'all' : ','.join([ str(i) for i in range(n_jobs)]),
            'idle' : ','.join([ str(i) for i in range(n_jobs)])
            }
        logger.info('Pushing new todo item %s: %s', cluster_id, new_item)
        new_todo[str(cluster_id)] = new_item
        self.write(new_todo)
        return cluster_id, TodoList(self.todofile)


class HTCondorTodoItem(object):
    """docstring for HTCondorTodoItem"""

    @classmethod
    def from_section(cls, section_title, section):
        instance = cls()
        instance.read(section_title, section)
        return instance

    def __init__(self):
        super(HTCondorTodoItem, self).__init__()
        self.states = [
            'idle',
            'running',
            'removed',
            'completed',
            'held',
            'transferring',
            'suspended',
            'done',
            'failed',
            ]
        self.jobs = []
        # 'Private' dict to efficiently look up a job by id and state
        # subject to change, and not part of the api
        self._jobs_by_procid = {}
        self._jobs_by_state = {}
        self.status = None

    def __repr__(self):
        return super(HTCondorTodoItem, self).__repr__().replace('object', 'object {0}'.format(self.cluster_id))

    def read(self, section_title, section):
        logger.info('Reading from section %s', section_title)
        self.section = section
        self.section_title = section_title
        # Mandatory keys
        self.cluster_id = self.section['cluster_id']
        self.submission_path = self.section['submission_path']
        # Keys with a default
        self.monitor_level = self.section.get('monitor_level', 'high')
        self.submission_time = self.section.get('submission_time', None)
        self.total_failure_count = int(self.section.get('total_failure_count', 0))
        self.total_resubmission_count = int(self.section.get('total_resubmission_count', 0))
        self.get_job_instances()
        return self

    def get_job_instances(self):
        """
        Creates list of empty jobs and fills according to what is specified in the section
        """
        # Create list of empty jobs
        logger.debug('Creating job instances for %s', self.cluster_id)
        self.all = self.read_section_key('all', required=True)
        logger.debug('All proc_ids: %s', self.all)
        for proc_id in sorted(self.all):
            job = HTCondorJob(self.cluster_id, proc_id)
            self.jobs.append(job)
            self._jobs_by_procid[proc_id] = job
        logger.debug('Created jobs: %s', self.jobs)
        # Fill in the found states
        for state in self.states:
            self._jobs_by_state[state] = []
            for proc_id in self.read_section_key(state):
                job = self._jobs_by_procid[proc_id]
                job.set_prev_state(state)
                self._jobs_by_state[state].append(job)
        # Count number of failed resubmission attempts
        for pair in self.read_section_key('failurecounts'):
            proc_id, count = pair.split(':')
            self._jobs_by_procid[proc_id].set_failurecount(count)

    def read_section_key(self, key, required=False):
        if not key in self.section:
            if required:
                raise ValueError(
                    'Key {0} is required but not found'
                    .format(key)
                    )
            return []
        if key in self.states or key == 'all':
            return [ int(i) for i in self.section[key].split(',') ]
        else:
            return self.section[key].split(',')

    def debug_log(self):
        """
        Sends state dict to log
        """
        # logger.debug(
        #     'State for {0}:\n{1}'
        #     .format(self.cluster_id, pprint.pformat(
        #         { s : [int(j.proc_id) for j in self._jobs_by_state[s]] for s in self.states }
        #         ))
        #     )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('Verbose output for %s:\n%s', self, pprint.pformat(vars(self)))

    def get_n_jobs(self):
        """
        Returns the total number of jobs tracked in this todoitem
        """
        return len(self.all)

    def get_state(self, job_id):
        return self._jobs_by_procid[job_id].prev_state

    def get_job(self, job_id):
        return self._jobs_by_procid[job_id]

    def get_jobs_in_state(self, state):
        if not state in self.states:
            raise ValueError(
                'No state {0} in available states {1}'
                .format(state, self.states)
                )
        return self._jobs_by_state[state]

    def copy(self):
        """
        Creates a deepcopy of the instance, with the exception of the 
        underlying HTCondorJob instances; these are the same instances
        """
        # Disable deepcopying of the HTCondorJob class
        def override_job_deepcopy(self, memo):
            logger.debug('deepcopy of %s is overridden, returning shallow', self.__class__)
            return self
        restore = False
        if hasattr(HTCondorJob, '__deepcopy__'):
            _backup_deepcopy = HTCondorJob.__deepcopy__
            restore = True
        HTCondorJob.__deepcopy__ = override_job_deepcopy
        # Throws error otherwise... kinda weird
        configparser.SectionProxy.__deepcopy__ = override_job_deepcopy
        # Create the deepcopy of HTCondorTodoItem instance
        self.debug_log()
        new = copy.deepcopy(self)
        # Restore deepcopying of HTCondorJob class if necessary
        if restore: HTCondorJob.__deepcopy__ = _backup_deepcopy
        return new

    def move(self, job, new_state):
        if not new_state in self.states:
            raise ValueError('State {0} does not exist'.format(new_state))
        current_state = job.prev_state
        if current_state == new_state:
            logger.info('Job %s state change: %s -> %s; doing nothing', job.proc_id, current_state, new_state)
        else:
            self._jobs_by_state[current_state].remove(job)
            self._jobs_by_state[new_state].append(job)
            job.prev_state = new_state
            logger.info('Job %s state change: %s -> %s', job.proc_id, current_state, new_state)

    def is_finished(self):
        """
        Counts done, failed, and all jobs, and determines whether the job is finished.
        Returns a dict.
        """
        done_jobs = [ j.proc_id for j in self.get_jobs_in_state('done')]
        failed_jobs = [ j.proc_id for j in self.get_jobs_in_state('failed')]
        n_done = len(done_jobs)
        n_failed = len(failed_jobs)
        n_all = self.get_n_jobs()
        if set(done_jobs + failed_jobs) == set(self.all):
            finished = True
            logger.info(
                'Todo item {0} is finished: {1} ({2:.2f}%) done, {3} ({4:.2f}%) failed'
                .format(self, n_done, (100.*n_done)/n_all, n_failed, (100.*n_failed)/n_all)
                )
        else:
            finished = False
        return {
            'finished' : finished,
            'n_done' : n_done,
            'n_failed' : n_failed,
            }

    def compute_status(self):
        """
        Sets the `status` attribute to a dict with high-level information regarding the job
        """
        self.status = self.is_finished()

    def parse_todoitem(self):
        """
        Returns a dict suitable for parsing to a todo file
        """
        # Required attributes
        r = {
            'cluster_id' : str(self.cluster_id),
            'submission_path' : self.submission_path,
            'total_failure_count' : str(self.total_failure_count),
            'total_resubmission_count' : str(self.total_resubmission_count)
            }
        # Optional attributes
        for key in [ 'monitor_level', 'submission_time', 'all' ]:
            if key in self.section: r[key] = self.section[key]
        # Parse states
        for state in self.states:
            jobs = self.get_jobs_in_state(state)
            if len(jobs) == 0: continue
            r[state] = ','.join([str(j.proc_id) for j in jobs])
        return r


class HTCondorJob(object):
    """docstring for HTCondorJob"""
    def __init__(self, cluster_id, proc_id):
        super(HTCondorJob, self).__init__()
        self.cluster_id = cluster_id
        self.proc_id = proc_id
        self.failurecount = 0
        self.stderr = None
        self.classad = None
        # Variables to keep track of what information is present for this job
        self._isset_prev_state = False
        self._isset_failurecount = False
        self._isset_queuestate = False
        self._isset_todoitem = False
        self._iscalled_history = False

    def set_parent_todoitem(self, todoitem):
        """
        Setter for the parent todoitem
        """
        self.todoitem = todoitem
        self._isset_todoitem = True

    def set_prev_state(self, state):
        """
        Setter for the prev_state attribute
        """
        self.prev_state = state
        self._isset_prev_state = True

    def set_failurecount(self, failurecount):
        """
        Setter for the failurecount attribute
        """
        self.failurecount = failurecount
        self._isset_failurecount = True

    def set_queuestate(self, queuestate, classad):
        self.queuestate = queuestate
        self.classad = classad
        self.new_state = self.classad.state
        self.schedd = self.classad.schedd
        self._isset_queuestate = True

    def __repr__(self):
        return super(HTCondorJob, self).__repr__().replace(
            'object',
            'object {0}.{1}'.format(self.cluster_id, self.proc_id)
            )

    def history(self):
        if not self._iscalled_history:
            self._history = cjm.utils.get_job_history_htcondor(self.cluster_id, self.proc_id)
            self._iscalled_history = True
        return self._history

    def spec(self):
        return '{0}.{1}'.format(self.cluster_id, self.proc_id)

    def _get_relative_stderr_filename(self):
        if self.classad and 'Err' in self.classad:
            err = self.classad['Err']
        else:
            history = self.history()
            if history and 'Err' in history:
                err = history['Err']
            else:
                logger.info('Could not get a path to a (relative) stderr file')
                return
        logger.info('Retrieved stderr file %s', err)
        return err

    def _get_stderr_filename(self):
        stderr_file = self._get_relative_stderr_filename()
        if stderr_file is None:
            return
        elif stderr_file.startswith('/'):
            logger.info('stderr_file %s looks like an absolute path')
        else:
            logger.info('stderr_file %s looks like a relative path; finding submission_path')
            # Need the submission path from the parent; path to stderr is typically relative
            if not self._isset_todoitem:
                logger.info('No parent todoitem set')
                return
            if not getattr(self.todoitem, 'submission_path', None):
                logger.info('Parent todoitem %s does not have a submission_path set', self.todoitem)
                return
            stderr_file = osp.join(osp.abspath(self.todoitem.submission_path), stderr_file)
            logger.info('Using stderr_file %s', stderr_file)
        return stderr_file

    def _get_stderr(self):
        stderr_file = self._get_stderr_filename()
        if not stderr_file:
            return False
        elif not osp.isfile(stderr_file):
            logger.warning('%s is not a file, cannot retrieve stderr')
            return False
        self.stderr_file = stderr_file
        self.stderr = cjm.utils.tail(stderr_file, 10)
        return True

    def get_stderr(self):
        if self.stderr is False:
            logger.debug('Retrieving stderr failed previously, not trying again')
            return None
        elif self.stderr is None:
            success = self._get_stderr()
            if not success:
                self.stderr = False
                return None
        return self.stderr

    def get_jobstatus_from_classad(self):
        if self.classad and 'JobStatus' in self.classad:
            return self.classad['JobStatus']
        else:
            logger.info('Could not determine JobStatus from classad for %s', self)
            return -1

    def get_jobstatus_from_history(self):
        history = self.history()
        if history and 'JobStatus' in history:
            return self.history['JobStatus']
        else:
            logger.info('Could not determine JobStatus from history for %s', self)
            return -1

    def get_jobstatus_from_classad_or_history(self):
        status = self.get_jobstatus_from_classad()
        if status == -1:
            status = self.get_jobstatus_from_history()
            if status == -1:
                logger.warning('Could not determine JobStatus for %s from htcondor at all', self)
        return status

    def get_exitcode(self):
        """
        Returns the exitcode if the job's history could be retrieved,
        or -1000 if there is a history but there was no key ExitCode,
        or -2000 if no history could be retrieved
        """
        history = self.history()
        if history:
            if 'ExitCode' in history:
                exitcode = int(history['ExitCode'])
                if exitcode == 0:
                    logger.info('Job %s completed succesfully', self)
                    return exitcode
                else:
                    logger.info('Job %s has non-zero exit code %s', self, exitcode)
                    return exitcode
            else:
                logger.info('Job %s has a history but no ExitCode; this usually means failed', self)
                return -1000
        else:
            logger.info('Job %s has no history to get ExitCode from', self)
            return -2000


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
        self._isread = False
        self.classads = []
        self._classads_by_procid = {}
        self._classads_by_state = {}

    def pformat(self):
        return self.__repr__()[:-1] + ' classads: ' + pprint.pformat(self._classads_by_state) + ' >'

    def xquery(self, projection=None, requirements=None, schedd=None):
        """
        Queries the htcondor queue utility. Yields an iterator of classads
        """
        if projection is None: projection = self.projection
        if requirements is None: requirements = self.requirements
        jobs = []
        # If the exact scheduler is known, just use it, but otherwise query all
        schedds = self.config.schedds if schedd is None else [schedd]
        for schedd in schedds:
            logger.debug('Querying %s, xquery: %s', schedd, schedd.xquery)
            for classad in schedd.xquery(
                requirements=self.requirements,
                projection=self.projection
                ):
                # Set a few helper attributes that are used often (saves querying the classad)
                classad.schedd = schedd
                classad.proc_id = int(classad['ProcId'])
                classad.state = int(classad.get('JobStatus', -1))
                logger.debug(
                    'Got %s.%s at %s from query: %s',
                    classad.__class__.__module__, classad.__class__.__name__, hex(id(classad)), classad
                    )
                yield classad

    def read(self):
        """
        Reads the state from the htcondor queue utility iterator
        """
        self.classads = list(sorted(self.xquery(), key=lambda j: j.proc_id))
        for classad in self.classads:
            self._classads_by_procid[classad.proc_id] = classad
            if not classad.state in self._classads_by_state: self._classads_by_state[classad.state] = []
            self._classads_by_state[classad.state].append(classad)
        self._isread = True
        return self

    def get_classad(self, proc_id):
        return self._classads_by_procid[proc_id]

    def get_state(self, proc_id):
        return self.get_classad(proc_id)

    def get_classads_in_state(self, state):
        if not state in self._classads_by_state:
            logger.debug(
                'No state %s in available states %s',
                state, self._classads_by_state.keys()
                )
            return []
        return self._classads_by_state[state]

    def has_proc_id(self, proc_id):
        """
        Checks whether a classad with proc_id is listed in this queue state.
        Returns boolean
        """
        return proc_id in self._classads_by_procid


class HTCondorUpdater(object):
    """
    Updates a todoitem (HTCondorTodoItem) from the todofile based on the
    current state of the htcondor queue (HTCondorQueueState).
    This class contains the main functionalities of this package.
    """
    def __init__(self, todoitem, queuestate, email=None):
        super(HTCondorUpdater, self).__init__()
        self.todoitem = todoitem
        self.queuestate = queuestate
        # Create a new todoitem, starting out as just a copy
        self.new_todoitem = self.todoitem.copy()
        self.email = email

    def update(self):
        logger.debug(
            'Constructing update for %s, %s',
            self.todoitem.section, self.todoitem.cluster_id
            )
        for job in self.todoitem.jobs:
            self.process(job)
        self.new_todoitem.compute_status()
        logger.debug('Newly created todo item after update:')
        self.new_todoitem.debug_log()
        # Send possible events to email.
        # The Email class will determine whether an email should actually be sent.
        self.email_event(cjm.EventCodes.cluster_finished, self.new_todoitem)
        self.email_event(cjm.EventCodes.monitoring, self.new_todoitem, old_todoitem=self.todoitem)
        return self.new_todoitem

    def email_event(self, event_code, todoitem, **kwargs):
        """
        If the HTCondorUpdater instance has an email attribute, this method
        registers that something noteworthy might have happened to the email.
        The email will determine exactly whether the event is noteworthy enough
        to warrant an actual email to be sent.
        """
        if not self.email: return
        self.email.make_event(event_code, todoitem, **kwargs)

    def message(self, job, msg):
        logger.debug(
            'Job %s: %s -> %s, %s',
            job.proc_id, job.prev_state, job.new_state, msg
            )

    def process(self, job):
        """
        Based on the current queuestate, this function changes the state of the job
        in the new_todoitem, analyzes failres, attempts resubmission of the job, and
        marks noteworthy events for a potential email.
        """
        # Look for a matching classad in the retrieved queue state
        if self.queuestate.has_proc_id(job.proc_id):
            classad = self.queuestate.get_classad(job.proc_id)
            job.set_queuestate(self.queuestate, classad)
            logger.info('Found matching classad %s for job %s', classad, job)
            classad_found = True
        else:
            logger.info('Job %s is not listed in the queuestate', job)
            classad_found = False
            job.new_state = 'unlisted'
            job.classad = None

        logger.debug(
            'Processing job %s: prev_state \'%s\', new_state \'%s\'',
            job.proc_id, job.prev_state, job.new_state
            )

        # Convenience functions for a few common job state changes
        same_state = lambda: self.message(job, 'state unchanged, doing nothing')
        no_further_action = lambda: self.message(job, 'state changed, no further action')

        # Go through all the scenario's of state changes
        if job.new_state == 1: # idle
            if job.prev_state == 'idle':
                same_state()
            else:
                no_further_action()
                self.new_todoitem.move(job, 'idle')
        elif job.new_state == 2: # running
            if job.prev_state == 'running':
                same_state()
            else:
                self.message(job, 'started running')
                self.new_todoitem.move(job, 'running')
        elif job.new_state == 3: # removed
            self.permanent_failure(job)
        elif job.new_state == 4: # completed
            if job.prev_state == 'done':
                same_state()
            elif job.prev_state == 'failed':
                self.message(job, 'previously marked as failed, doing nothing')
            else:
                exitcode = job.get_exitcode()
                if exitcode == -2000 or exitcode == 0:
                    logger.info('Marking job %s as succesfull', job)
                    self.new_todoitem.move(job, 'done')
                else:
                    self.attempt_resubmission(job)
        elif job.new_state == 5: # held
            self.attempt_resubmission(job)
        elif job.new_state == 6: # transferring
            if job.prev_state == 'transferring':
                same_state()
            else:
                self.message(job, 'started running')
                self.new_todoitem.move(job, 'transferring')
        elif job.new_state == 7: # suspended
            if job.prev_state == 'failed':
                self.message(job, 'previously marked as failed, doing nothing')
            else:
                self.permanent_failure(job)
        elif job.new_state == 'unlisted': # job is not in the queuestate
            if job.prev_state == 'done' or job.prev_state == 'failed':
                self.message(
                    job,
                    'unlisted and previous state was {0}, doing nothing'.format(job.prev_state)
                    )
            else:
                exitcode = job.get_exitcode()
                if exitcode == -2000 or exitcode == 0:
                    logger.info('Marking job %s as succesfull', job)
                    self.new_todoitem.move(job, 'done')
                else:
                    self.attempt_resubmission(job)
        else:
            self.message(job, 'no action implemented, doing nothing')

    def attempt_resubmission(self, job):
        logger.debug('Analyzing failure for job %s', job)
        job.failurecount += 1
        if job.classad:
            if 'HoldReasonCode' in job.classad and int(job.classad['HoldReasonCode']) == 34:
                used_memory = job.classad.get('MemoryUsage', '?')
                request_memory = job.classad.get('RequestMemory', '?')
                new_request_memory = int(2*int(request_memory)) if not request_memory == '?' else 4096
                logger.info(
                    'Job %s failed because it exceeded the memory limit: '
                    'MemoryUsage = %s, RequestMemory = %s. '
                    'Attempting to resubmit with twice as much memory: %s',
                    job, used_memory, request_memory, new_request_memory
                    )
                job.schedd.edit(
                    job.spec(),
                    'RequestMemory', new_request_memory
                    )
                job.schedd.act(htcondor.JobAction.Release, job.spec())
                logger.info('Made edit call the schedd %s', job.schedd)
                self.new_todoitem.move(job, 'idle')
                self.email_event(
                    cjm.EventCodes.job_resubmitted,
                    self.new_todoitem,
                    job = job,
                    details = (
                        'Resubmitted with RequestMemory = %s '
                        '(previously MemoryUsage = %s, RequestMemory = %s)',
                        new_request_memory, used_memory, request_memory
                        ),
                    current_resubmission_count = self.new_todoitem.total_resubmission_count
                    )
                self.new_todoitem.total_resubmission_count += 1
                return
        self.permanent_failure(job)

    def permanent_failure(self, job):
        self.message(job, 'failed with no resubmission options')
        history = job.history()
        if history:
            logger.info(
                'Some possibly noteworthy information from the history:\n%s',
                pprint.pformat({
                    key : history[key] for key in cjm.CONFIG.interesting_history_keys if key in history
                    })
                )
        if job.classad:
            logger.info(
                'Some possibly noteworthy information from the classad:\n%s',
                pprint.pformat({
                    key : job.classad[key] for key in cjm.CONFIG.interesting_history_keys if key in job.classad
                    })
                )
        stderr = job.get_stderr()
        if stderr:
            logger.info('Tail of %s:\n%s', job.stderr_file, stderr)
        self.new_todoitem.move(job, 'failed')
        self.email_event(
            cjm.EventCodes.job_permanently_failed,
            self.new_todoitem,
            job = job,
            current_failure_count = self.new_todoitem.total_failure_count
            )
        self.new_todoitem.total_failure_count += 1
























