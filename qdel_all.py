#!/usr/bin/env python

import os
import sys

import pbs

def kill_all_user_jobs(username):
    for job in pbs.qstat(user=username):
        print 'Killing ', job
        pbs.qdel(job)

def main(argv=sys.argv[1:]):
    kill_all_user_jobs(os.getenv('USER'))

if __name__ == "__main__":
    main()
