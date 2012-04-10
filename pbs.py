"""Utilities for submitting and managing pbs batch scripts."""

import subprocess
import os
import time
import uuid

import configuration

class PBSUtilError(Exception): pass
class PBSUtilQStatError(PBSUtilError): pass
class PBSUtilQSubError(PBSUtilError): pass
class PBSUtilWaitError(PBSUtilError): pass

class JobStatus:
    
    def __init__(self, id, state, name=None, elapsed_time=None,
                 username=None):
        self.id = id
        self.state = state
        self.name = name
        self.elapsed_time = elapsed_time
        self.username = username

    def __str__(self):
        return '%10s %20s         %s   %s' % (self.id, self.name, self.state, self.elapsed_time)
    

def call_qstat(args):
    """Execute qstat, and return output lines"""
    qstat_process = subprocess.Popen(["qstat"] + args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return qstat_process.communicate()[0].splitlines()


def parse_qstat_plain_output(output_lines):
    """Parse the output of qstat in the form with no arguments."""
    
    if len(output_lines) < 3:
        raise PBSUtilQStatError('Bad qstat output:\n"%s"' % '\n'.join(output_lines))

    job_statuses = []

    for output_line in output_lines[2:]:
        job_record = output_line.split()
        record_job_id = parse_qsub_output(job_record[0])[0]
        record_job_state = job_record[4]
        name = job_record[1]
        elapsed_time = job_record[3]
        job_statuses.append(JobStatus(record_job_id, record_job_state, name=name, elapsed_time=elapsed_time))

    return job_statuses


def parse_qstat_all_output(output_lines):
    """Parse the output of qstat in the form with the -a argument."""
    
    if len(output_lines) < 3:
        raise PBSUtilQStatError('Bad qstat output:\n"%s"' % '\n'.join(output_lines))

    job_statuses = []

    for output_line in output_lines[5:]:
        job_record = output_line.split()
        record_job_id = parse_qsub_output(job_record[0])[0]
        record_job_state = job_record[8]
        name = job_record[3]
        elapsed_time = job_record[10]
        username = job_record[1]
        job_statuses.append(JobStatus(record_job_id, record_job_state, name=name, elapsed_time=elapsed_time,
                                      username=username))

    return job_statuses
    

def qstat_plain():
    """Return a JobStatus object output by qstat for empty argument line."""
    output_lines = call_qstat(['-a'])

    job_statuses = parse_qstat_all_output(output_lines)
    
    return job_statuses

    
    
def qstat_id(job_id):
    """Return a JobStatus object output by a qstat ### request.

    The output for qstat is very different depending on the query, so
    the different queries have been broken into distinct
    functions.
    
    """

    output_lines = call_qstat([str(job_id)])
    if len(output_lines) != 3:
        raise PBSUtilQStatError('Bad qstat id output:\n"%s"' % '\n'.join(output_lines))

    job_statuses = parse_qstat_plain_output(output_lines)
    
    assert len(job_statuses) == 1, "qstat id did not return the expected number of job statuses: %s != 1" % len(job_statuses)

    job_stat = job_statuses[0]
    assert job_stat.id == job_id, "qstat job_id did no match expected job_id.  %s != %s" % (job_id, record_job_id)

    return job_stat
        
def qstat_user(user):
    """Return a JobStatus object output by a qstat -u user request..

    The output for qstat is very different depending on the query, so
    the different queries have been broken into distinct
    functions.
    
    """
    
    job_stats = []

    output_lines = call_qstat(['-u', user])
    if len(output_lines) < 4:
        return job_stats               # No jobs for the current user
    for line in output_lines[5:]:
        job_record = line.split()
        record_job_id = parse_qsub_output(job_record[0])[0]
        record_job_state = job_record[9]
        job_stats.append(JobStatus(record_job_id, record_job_state, name=job_record[3], elapsed_time=job_record[10]))
    return job_stats

def qstat(job_id=None, user=None):
    """Return JobStatus objects from output of qstat with desired options."""

    if job_id:
        return [qstat_id(job_id)]
    elif user:
        return qstat_user(user)
    else:
        return qstat_plain()


def parse_qsub_output(output):
    """Divide qsub output into a tuple of job_id and hostname signature."""
    try:
        job_id = output.split('.')[0]
        signature = '.'.join(output[:-1].split('.')[1:]) # the [:-1] kills the newline at the end of the qsub output
        return (job_id, signature)
    except Exception:
        raise PBSUtilQSubError('Unable to parse qsub output: "%s"' % output)

    
def qsub(script_filename, verbose=False):
    """Submit the given pbs script, returning the jobid."""
    cmd = ["qsub", script_filename]
    qsub_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    qsub_output_pipes = qsub_process.communicate()
    qsub_output = qsub_output_pipes[0]
    if len(qsub_output) == 0:
        raise PBSUtilQSubError("Failed to submit %s, qsub gave no stdoutput.  stderr: '%s'" % (script_filename, qsub_output_pipes[1]))
    if verbose:
        print '\n%s\n' %  qsub_output
    pbs_id = parse_qsub_output(qsub_output)[0]
    return pbs_id

def qwait(job_id,sleep_interval=5,max_wait=None):
    try:
        while qstat_id(job_id).state == 'Q':
            time.sleep(sleep_interval)
    except PBSUtilError:
        return

    if not max_wait is None:
        start_time = time.time()
    while True:
        if (not max_wait is None) and time.time() - start_time > max_wait:
            raise PBSUtilWaitError("PBS script failed to return within max_wait time. max_wait=%s" % max_wait)
        try:
            qstat_id(job_id)       # This will throw an exception when the job completes.
            time.sleep(sleep_interval)
        except PBSUtilError:
            break

def qdel(job_id):
    """Kill the given pbs jobid."""
    if isinstance(job_id, JobStatus):
        qdel_process = subprocess.Popen(["qdel", str(job_id.id)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        qdel_process = subprocess.Popen(["qdel", str(job_id)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def temp_file_name(suffix):
    """Return a new temporary file name."""
    return 'tmp%s%s' % (uuid.uuid4(), suffix)

def get_signature():
    dummy_script_name = temp_file_name('dummy_script')
    open(dummy_script_name, 'w')
    try:
        qsub_process = subprocess.Popen(["qsub", dummy_script_name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (job_id, signature) = parse_qsub_output(qsub_process.communicate()[0])
        qdel(job_id)
    finally:
        os.remove(dummy_script_name)


    signature = '.'.join(signature.split('.')[:1])

    return signature


def generic_script(contents, 
                   job_name=None, 
                   stdout='/dev/null', 
                   stderr='/dev/null', 
                   shebang='#!/bin/bash',
                   numnodes=None,
                   numcpu=None,
                   queue=None,
                   walltime=None,
                   mem=None,
                   pmem=None):
    """Create a generic pbs script executing contents."""
    me = __file__
    current_time = time.strftime('%H:%M %D')

    if job_name is None:
        job_name = 'unnamed_job'

    if numnodes is None:
        numnodes = str(configuration.numnodes)

    if numcpu is None:
        numcpu = str(configuration.numprocs)

    if pmem:
        pmem = ',pmem=' + pmem
    else:
        pmem=''


    if mem:
        mem = ',mem=' + mem
    else:
        mem=''


    if queue is None:
        queue = configuration.queue

    additional_configuration_lines = []

    if queue is not None:
        additional_configuration_lines.append("#PBS -q %(queue)s" % locals())
        
    if walltime is None:
        walltime = configuration.walltime

    if walltime is not None:
        additional_configuration_lines.append("#PBS -l walltime=%(walltime)s" % locals())

    additional_configuration =  '\n'.join(additional_configuration_lines)

    the_script = """%(shebang)s
# Created by %(me)s at %(current_time)s
#PBS -V
#PBS -N %(job_name)s
#PBS -l nodes=%(numnodes)s:ppn=%(numcpu)s%(pmem)s%(mem)s
#PBS -o %(stdout)s
#PBS -e %(stderr)s
%(additional_configuration)s

%(contents)s
""" % locals()

    return the_script

def strip_pbs_ids(source):
    """Return a list of the pbs job ids contained in source."""
    signature = get_signature()
    return [qsub_output.split('.')[0] for qsub_output in source.splitlines() if qsub_output.find(signature) > 0]

