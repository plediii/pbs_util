#!/usr/bin/env python

import sys
import argparse

import pbs

def kill_all(source):
    for jobid in pbs.strip_pbs_ids(source):
        print 'Killing %s' % jobid
        pbs.qdel(jobid)
    

def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser("""%prog 

Kill all jobs listed in stdin.""")

    parser.parse_args(argv)

    kill_all(sys.stdin.read())

if __name__ == "__main__":
    main(sys.argv[1:])
