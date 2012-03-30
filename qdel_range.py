#!/usr/bin/env python
"""Delete pbs jobs in range min to max."""

import sys

import pbs

def qdel_range(min_id, max_id):
    for job_stat in pbs.qstat():
        if int(job_stat.id) >= min_id and int(job_stat.id) <= max_id:
            print 'Deleting %s ' % job_stat
            pbs.qdel(job_stat)

def main(argv):
    if len(argv) != 2:
        print 'Please provide min and max job ids to kill.'
        sys.exit(1)

    min_id, max_id = [int(x) for x in argv]

    print 'Killing jobs %s to %s' % (min_id, max_id)

    qdel_range(min_id, max_id)


if __name__ == "__main__":
    main(sys.argv[1:])
