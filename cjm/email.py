#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging, os, glob
import os.path as osp
import cjm
logger = logging.getLogger('cjm')


class EventCodes(object):
    """
    Available event codes
    """
    job_permanently_failed = 'job_permanently_failed'
    job_resubmitted = 'job_resubmitted'
    cluster_finished = 'cluster_finished'
    monitoring = 'monitoring'


class Email(object):
    """docstring for Email"""
    def __init__(self):
        super(Email, self).__init__()
        self.todoitems = {}
        self.events = []
        
    def get_section(self, todoitem):
        if not todoitem in self.todoitems:
            logger.debug(
                'Creating new EmailTodoItemSection for TodoItem %s', todoitem
                )
            self.todoitems[todoitem] = EmailTodoItemSection(todoitem)
        return self.todoitems[todoitem]

    def iter_sections(self):
        for todoitem in self.todoitems:
            yield self.get_section(todoitem)

    def make_event(self, event_code, todoitem, **kwargs):
        self.events.append(
            ( event_code, todoitem, kwargs )
            )

    def process_event(self, event):
        """
        Checks whether an event is actually noteworthy, and if so, registers a message
        """
        event_code, todoitem, kwargs = event
        section = self.get_section(todoitem)
        section.process_event(event_code, kwargs)
        
    def compile_email_text(self):
        # Move all 'monitoring' event codes to the end; 'monitoring' should trigger
        # as well if any other event happened
        self.events.sort(key=lambda event: -1 if event[0] == EventCodes.monitoring else 0)
        for event in self.events:
            self.process_event(event)

        text = []
        for section in self.iter_sections():
            for priority, message in sorted(section.messages):
                text.append(message)
        if len(text) == 0:
            logger.debug('No noteworthy event happened, not sending email')
            return False
        return '\n'.join(text)

    def send_email(self):
        email_text = self.compile_email_text()
        if email_text is False: return
        logger.debug('Sending the following text in an email:\n%s', email_text)
        cmd = ['mail -s "cjm update" tklijnsm@gmail.com <<< "{0}"'.format(
            email_text.replace('"', '').replace("'", '')
            )]
        cjm.utils.run_command(cmd, shell=True)


class EmailTodoItemSection(object):
    """docstring for EmailTodoItemSection"""

    event_code_to_method = {
        EventCodes.cluster_finished       : 'cluster_finished',
        EventCodes.monitoring             : 'monitoring',
        EventCodes.job_resubmitted        : 'job_resubmitted',
        EventCodes.job_permanently_failed : 'job_permanently_failed',
        }

    def __init__(self, todoitem):
        super(EmailTodoItemSection, self).__init__()
        self.todoitem = todoitem
        self.messages = []        

    def process_event(self, event_code, kwargs):
        output = getattr(self, event_code)(kwargs)
        if output is False:
            logger.debug(
                'Event %s for todoitem %s was not noteworthy, not sending an email',
                event_code, self.todoitem
                )
            return
        self.messages.append(output)

    def cluster_finished(self, kwargs):
        if not self.todoitem.status: self.todoitem.compute_status()
        if not self.todoitem.status['finished']: return False
        n_done = self.todoitem.status['n_done']
        n_failed = self.todoitem.status['n_failed']
        n_all = self.todoitem.get_n_jobs()
        message = (
            'Cluster {0} is finished: {1} ({2:.2f}%) done, {3} ({4:.2f}%) failed'
            .format(
                self.todoitem.cluster_id, n_done, (100.*n_done)/n_all, n_failed, (100.*n_failed)/n_all
                )
            )
        return 80, message
        
    def monitoring(self, kwargs):
        if not(
            getattr(self.todoitem, 'monitor_level', 'low') in ['high']
            or
            len(self.messages) > 0
            ):
            return False
        message = [[ '', 'previous', 'now' ]]
        for state in self.todoitem.states:
            old = str(len(kwargs['old_todoitem'].get_jobs_in_state(state)))
            new = str(len(self.todoitem.get_jobs_in_state(state)))
            message.append([state, old, new])
        message = '\n'.join([ ' '.join(line) for line in message ])
        message = 'Cluster {0}\n'.format(self.todoitem.cluster_id) + message
        return -10, message

    def job_resubmitted(self, kwargs):
        if not 'current_resubmission_count' in kwargs:
            logger.debug(
                'No resubmission count was given, will not email for todoitem %s (%s)',
                self.todoitem, EventCodes.job_resubmitted
                )
            return False
        elif kwargs['current_resubmission_count'] > cjm.CONFIG.email_for_first_n_resubmissions:
            logger.debug(
                'Resubmission count %s > %s, not sending email (%s)',
                kwargs['current_resubmission_count'], cjm.CONFIG.email_for_first_n_resubmissions,
                EventCodes.job_resubmitted
                )
            return False
        message = 'Job {0}: {1}'.format(
            kwargs['job'].proc_id, kwargs.get('details', 'Resubmitted (no details)')
            )
        return 10, message

    def job_permanently_failed(self, kwargs):
        if not 'current_failure_count' in kwargs:
            logger.debug(
                'No failure count was given, will not email for todoitem %s (%s)',
                self.todoitem, EventCodes.job_permanently_failed
                )
            return False
        elif kwargs['current_failure_count'] > cjm.CONFIG.email_for_first_n_failures:
            logger.debug(
                'Failure count %s > %s, not sending email (%s)',
                kwargs['current_failure_count'], cjm.CONFIG.email_for_first_n_failures,
                EventCodes.job_permanently_failed
                )
            return False

        job = kwargs['job']
        message = ['Details for failure of job {0}:'.format(job.proc_id)]

        history = job.history()
        if history:
            message.append('History: ' + ', '.join(
                ['{0}: {1}'.format(key, history[key]) for key in cjm.CONFIG.interesting_history_keys if key in history]
                ))

        if job.classad:
            message.append('ClassAd: ' + ', '.join(
                ['{0}: {1}'.format(key, job.classad[key]) for key in cjm.CONFIG.interesting_history_keys if key in job.classad]
                ))

        job.get_stderr()
        if job.stderr:
            message.append('Tail of {0}:\n{1}'.format(job.stderr_file, job.stderr))

        message = '\n'.join(message)
        return 20, message



