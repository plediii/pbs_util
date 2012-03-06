#!/usr/bin/env python
"""Wait for jobs listed in stdin to complete."""

import sys

import pbs

def wait_for_jobs(job_ids):
    for job_id in job_ids:
        pbs.qwait(job_id=job_id)
    
def main(argv=sys.argv[1:]):
    wait_for_jobs(pbs.strip_pbs_ids(sys.stdin))

