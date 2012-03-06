#!/usr/bin/env python
import time
import socket
import urllib
import json

import pbs

def trim_digits(s):
    return ''.join(c for c in s if not c.isdigit())

def get_hostname():
    full_hostname = socket.gethostname()
    hostname=full_hostname.split('.')[-3]

    hostname = trim_digits(hostname)

    return hostname


hostname=get_hostname()

def pbs_job_to_job_info(j):
    global hostname
    return {'hostname': hostname,
            'username': j.username,
            'jobid': str(j.id),
            'jobname': j.name,
            'time': j.elapsed_time}

def pbs_state():
    global hostname
    job_infos = [pbs_job_to_job_info(j) for j in pbs.qstat_plain()]

    return {'hostname': hostname,
            'job_infos': job_infos}



def report():
    global hostname
    state = pbs_state()

    state_json = json.dumps(state)

    params = urllib.urlencode({'state': state_json})

    try:
        f = urllib.urlopen("http://localhost/update", params)
        response = f.read()
    except IOError as e:
        response = e
    print '%s %s' % (hostname, response)


if __name__ == "__main__":
    while True:
        report()
        time.sleep(5 * 60)
