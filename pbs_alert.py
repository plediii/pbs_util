#!/usr/bin/env python

import sys
import os
import time

import pbs
import send_email

def jobs_running():
    return 0 < len(pbs.qstat(user=os.environ['USER']))

def send_alert(alert_msg):
    send_email.send(alert_msg)


def main(argv=[]):

    if argv == []:
        alert_msg = "No processes are running."
    else:
        alert_msg = ' '.join(argv)
    
    if not jobs_running():
        print 'Nothing is running.'
        sys.exit(1)

    pid = os.fork()

    if pid != 0:            # The parent
        print "Alert PID =  ", pid
        print 'Alert will be sent.'
        sys.exit(0)

    # the child

    while jobs_running():
        time.sleep(5*60)

    send_alert(alert_msg)


if __name__ == "__main__":
    main(sys.argv[1:])

