#!/usr/bin/env python

import os
import sys

import argparse

import pbs

def kill_all_jobs_named(username, name):
    for job in pbs.qstat(user=username):
        if job.name.find(name) >= 0:
            print 'Killing ', job
            pbs.qdel(job)

def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser(description="Kill all jobs which contain name as a substring.")
    parser.add_argument('name')
    
    args = parser.parse_args(argv)

    kill_all_jobs_named(os.getenv('USER'), args.name)

if __name__ == "__main__":
    main()
