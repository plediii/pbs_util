
import time
import web

import json

urls = (
    '/', 'index',
    '/get_state', 'get_state',
    '/get_hosts', 'get_hosts',
    '/update', 'update'
    )

app = web.application(urls, globals())

class index:
    def GET(self):
        return open('static/pbsmon.html').read()


class JobInfos(object):

    job_infos = []
    hosts = {}                  # dict of expire time for hosts

    def remove_host(self, hostname):
        
        new_job_infos = [job for job in self.job_infos if job['hostname'] != hostname]
        num_deleted = len(self.job_infos) - len(new_job_infos)
        self.job_infos = new_job_infos

        return num_deleted

    def remove_stale(self):
        """Remove jobs from any hostname that hasn't updated."""
        hosts = self.hosts
        hostnames = hosts.keys()
        for hostname in hostnames:
            if hosts[hostname] < time.time():
                del hosts[hostname]
                self.remove_host(hostname)
                print 'EXPIRED:', hostname

    def update(self, hostname, new_job_infos):
        self.remove_stale()
        self.remove_host(hostname)

        self.hosts[hostname] = time.time() + 60 * 10 # set to expire 10 minutes from now
        self.job_infos.extend(new_job_infos)

        print self.hosts


    def hostnames(self):
        return self.hosts.keys()
        

job_infos = JobInfos()


class get_state:
    def GET(self):
        global job_infos
        web.header('Content-Type', 'application/json')
        job_infos.remove_stale()
        return json.dumps(job_infos.job_infos)


class get_hosts:
    def GET(self):
        global job_infos
        web.header('Content-Type', 'application/json')
        job_infos.remove_stale()

        print 'get hosts'
        return json.dumps(job_infos.hostnames())



class update:
    def POST(self):
        i = web.input()

        state = json.loads(i.state)

        job_infos.update(state['hostname'], state['job_infos'])

        return '%s jobs.' % len(state['job_infos'])


if __name__ == "__main__":
    app.run()
