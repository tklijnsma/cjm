#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, shutil, logging, sys, subprocess, re
import os.path as osp
import cjm
logger = logging.getLogger('cjm')

def _create_directory_no_checks(dirname, dry=False):
    """
    Creates a directory without doing any further checks.

    :param dirname: Name of the directory to be created
    :type dirname: str
    :param dry: Don't actually create the directory, only log
    :type dry: bool, optional
    """
    logger.warning('Creating directory {0}'.format(dirname))
    if not dry: os.makedirs(dirname)

def create_directory(dirname, force=False, must_not_exist=False, dry=False):
    """
    Creates a directory if certain conditions are met.

    :param dirname: Name of the directory to be created
    :type dirname: str
    :param force: Removes the directory `dirname` if it already exists
    :type force: bool, optional
    :param must_not_exist: Throw an OSError if the directory already exists
    :type must_not_exist: bool, optional
    :param dry: Don't actually create the directory, only log
    :type dry: bool, optional
    """
    if osp.isfile(dirname):
        raise OSError('{0} is a file'.format(dirname))
    isdir = osp.isdir(dirname)

    if isdir:
        if must_not_exist:
            raise OSError('{0} must not exist but exists'.format(dirname))
        elif force:
            logger.warning('Deleting directory {0}'.format(dirname))
            if not dry: shutil.rmtree(dirname)
        else:
            logger.warning('{0} already exists, not recreating')
            return
    _create_directory_no_checks(dirname, dry=dry)

class switchdir(object):
    """
    Context manager to temporarily change the working directory.

    :param newdir: Directory to change into
    :type newdir: str
    :param dry: Don't actually change directory if set to True
    :type dry: bool, optional
    """
    def __init__(self, newdir, dry=False):
        super(switchdir, self).__init__()
        self.newdir = newdir
        self._backdir = os.getcwd()
        self._no_need_to_change = (self.newdir == self._backdir)
        self.dry = dry

    def __enter__(self):
        if self._no_need_to_change:
            logger.info('Already in right directory, no need to change')
            return
        logger.info('chdir to {0}'.format(self.newdir))
        if not self.dry: os.chdir(self.newdir)

    def __exit__(self, type, value, traceback):
        if self._no_need_to_change:
            return
        logger.info('chdir back to {0}'.format(self._backdir))
        if not self.dry: os.chdir(self._backdir)

def run_command(cmd, env=None, dry=False, shell=False):
    """
    Runs a command using subprocess, and logs and returns output.

    :param cmd: Command to run as a list of arguments
    :type cmd: str
    :param env: Dict with environment variables to set for this command only
    :type env: dict
    :param dry: If set to `True`, the command is only printed
    :type dry: bool
    :param shell: Executes the command in shell-mode (see `subprocess` for details)
    :type shell: bool
    """
    logger.warning('Issuing command: {0}'.format(' '.join(cmd)))
    if dry: return
    if shell:
        cmd = ' '.join(cmd)
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        universal_newlines=True,
        shell=shell
        )

    output = []
    for stdout_line in iter(process.stdout.readline, ""):
        stdout_line = stdout_line.rstrip('\n')
        logger.info('[STDOUT] ' + stdout_line)
        output.append(stdout_line)

    process.stdout.close()
    process.wait()
    returncode = process.returncode
    if returncode == 0:
        logger.info('Command exited with status 0 - all good')
    else:
        logger.error('Exit status {0} for command: {1}'.format(returncode, cmd))
        raise subprocess.CalledProcessError(cmd, returncode)
    return output

def get_job_history_htcondor(cluster_id, proc_id, schedd=None, projection=None):
    logger.debug('Getting history for job %s.%s, schedd %s', cluster_id, proc_id, schedd)
    import htcondor
    projection = [] if projection is None else projection
    if schedd is None:
        logger.debug('No scheduler specified, looking in all schedds')
        schedds = cjm.CONFIG.schedds
    else:
        schedds = [schedd]
    # Get jobs from all needed schedulers
    jobs = []
    for schedd in schedds:
        jobs.extend(list(schedd.history(
            requirements = 'ClusterId == {0} && ProcId == {1}'.format(cluster_id, proc_id),
            projection = projection,
            )))
    if len(jobs) > 1:
        logger.warning(
            'Unexpected: Found %s jobs matching cluster_id %s and proc_id %s',
            len(jobs), cluster_id, proc_id
            )
        return jobs
    elif len(jobs) == 0:
        logger.info(
            'No job retrieved from history for cluster_id %s and proc_id %s',
            cluster_id, proc_id
            )
        return
    else:
        return jobs[0]

def tail(file, n=10):
    """
    Reads last n lines of a file using the GNU tail utility
    """
    return run_command(['tail', '-n', str(n), file])


def submit(command_line):
    """
    Substitute for the condor_submit command. Expects a list, and
    will convert to a 1 element list if given a string.
    (Since it is passed as a shell=True command to subprocess, it
    does not matter)
    condor_submit is wrapped on LPC, so for now keep this command
    line option rather than using the python config
    """
    from six import string_types
    if isinstance(command_line, string_types):
        command_line = [command_line]
    if not command_line[0].startswith('condor_submit'):
        command_line.insert(0, 'condor_submit')
    output = run_command(command_line, shell=True)

    for line in output:
        match = re.match(r'(\d+) job\(s\) submitted to cluster (\d+)\.', line)
        if match:
            n_jobs = int(match.group(1))
            cluster_id = int(match.group(2))
    logger.info('Submitted %s jobs to cluster_id %s', n_jobs, cluster_id)
    return cluster_id, n_jobs, output

def remove(cluster_id):
    import htcondor
    logger.info('Removing cluster_id %s from queue', cluster_id)
    for schedd in cjm.CONFIG.schedds:
        schedd.act(htcondor.JobAction.Remove, 'ClusterId=={0}'.format(cluster_id))

