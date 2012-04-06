#!/usr/bin/env python

import sys

import time
import socket
import urllib
import json
import argparse

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



def report(update_host, update_url='/update'):
    global hostname

    update_url = 'http://' + update_host + update_url

    state = pbs_state()

    state_json = json.dumps(state)

    params = urllib.urlencode({'state': state_json})

    try:
        f = urllib.urlopen(update_url, params)
        response = f.read()
    except IOError as e:
        response = 'ERROR:' + str(e)

    max_response = 500
    if len(response) > max_response:
        response = 'Long response; tail = ' + response[-max_response:]

    print '%s %s' % (hostname, response)


def main(argv=sys.argv[1:]):
    
    parser = argparse.ArgumentParser(description="""Update a pbsmon server.""")

    parser.add_argument("host", help="Host running pbsmon server.")
    parser.add_argument("--port", default="8080", 
                        help="Port on the host running the pbsmon server.")


    args = parser.parse_args(argv)

    update_host = args.host + ':' + args.port

    print '^C to stop.'

    while True:
        report(update_host)
        time.sleep(5 * 60)
    

if __name__ == "__main__":
    main()
