#! /usr/bin/env python
"""Nicely submit lots of jobs to a PBS queue.

Here nice means, only submit a job if you have less than `max_submissions' already submitted."""

from __future__ import with_statement

import os
import sys
import time
import traceback
import optparse
import socket

import pbs


import configuration

## Parameters
max_submissions=configuration.max_submissions

assert os.environ.has_key('USER'), " unable to determine username."

class NiceSubmitError(Exception):
    pass

class ReachedMax(NiceSubmitError):
    pass

class QSubFailure(NiceSubmitError):
    pass

def submit_files_until_done(filenames, wait_for_all=False, delay_check=0.5, sleep_seconds=60 * 5, 
                            quiet=False, 
                            fail_when_max=False, 
                            retry_on_failure=True):
    global max_submissions
    submitted_ids = []
    num_to_submit = len(filenames)
    while filenames:
        num_submitted = len(pbs.qstat(user=os.environ['USER']))
        if (num_submitted < max_submissions):
            if os.path.exists(filenames[0]):
                try:
                    job_id = pbs.qsub(filenames[0], verbose=not quiet) 
                    if delay_check:
                        time.sleep(delay_check)
                    pbs.qstat(job_id=job_id) # If this doesn't throw, then it was submitted successfully
                    if not quiet:
                        print 'Submitted %s as "%s" at %s  (%s/%s left to submit)' % (filenames[0], job_id, time.asctime(), len(filenames[1:]), num_to_submit)
                    filenames = filenames[1:]
                    submitted_ids.append(job_id)
                    num_submitted = num_submitted + 1
                    if not quiet:
                        print 'I think submitted %d/%d' % (num_submitted,max_submissions)
                    sys.stderr.flush()
                    sys.stdout.flush()
                except pbs.PBSUtilError:
                    traceback.print_exc()
                    if not quiet:
                        print 'Failed to submit %s at %s  (%s left to submit)' % (filenames[0], time.asctime(), len(filenames[1:]))
                    sys.stderr.flush()
                    sys.stdout.flush()

                    if not retry_on_failure:
                        raise QSubFailure()

                    time.sleep(max(int(round(sleep_seconds/2)), 1))
                    # Maybe we saturated the queue.
            else:
                if not quiet:
                    print 'ERROR: Cannot submit %s because it does not exist.' % filenames[0]
                sys.stderr.flush()
                sys.stdout.flush()

                filenames = filenames[1:]
        else:
            if fail_when_max:
                raise ReachedMax()
            sys.stdout.write('Queue is currently full.')
            sys.stdout.flush()
            time.sleep(sleep_seconds)
    if wait_for_all:
        for job_id in submitted_ids:
            pbs.qwait(job_id)
    return submitted_ids

def main(argv):
    parser = optparse.OptionParser(usage="usage: %prog [options] script_list_file")
    parser.add_option('-o', dest="outfile", 
                      help="Run as a daemon, and send output to outfile.")
    parser.add_option('-m', dest='maxjobs', type="int",
                      help="Maximum number of jobs to be simultaneously submitted.")
    parser.add_option('-c', dest='delay_check', type="int", default=0,
                      help="Number of seconds to delay before double checking that the job ended up on the queue and reporting success.  This increases the time required to submit all the jobs, but is more robust against failure.")

    parser.add_option('-s', dest='sleep_seconds', type="int", default=60*5,
                      help="Number of seconds to sleep between checking to submit more jobs.")


    (options, args) = parser.parse_args(argv)
    if len(args) != 1:
        parser.error("incorrect number of arguments.")

    if options.maxjobs:
        max_submissions=options.maxjobs

    script_list_file_name = args[0]
    script_files = [name.strip() for name in open(script_list_file_name).readlines() if len(name.strip()) > 0] # -1 to remove the trailing \n

    print 'Submitting %s jobs.' % len(script_files)

    if options.outfile is not None:
        outfile = options.outfile
    else:
        outfile = script_list_file_name + '.out'
        

    if parser.outfile:
        pid = os.fork()
        if pid != 0:            # The parent
            print time.asctime()
            print "submitter PID =  ", pid
            print "logging to '%s'" % outfile
            sys.exit(0)
        else:                   # The child
            sys.stdout = open(outfile, 'w')
            sys.stderr = sys.stdout
            print time.asctime()
            print 'Hostname = ', socket.gethostname()
            print "PID = ", os.getpid()

    print 'Submitting %s jobs.' % len(script_files)
        
    submit_files_until_done(script_files,delay_check=options.delay_check, sleep_seconds=options.sleep_seconds)
    print 'Finished submitting.'

if __name__ == "__main__":
    main(sys.argv[1:])

    



