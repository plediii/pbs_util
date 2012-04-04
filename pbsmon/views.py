
import os

from django.views.decorators.csrf import csrf_response_exempt, csrf_exempt
from django.shortcuts import render_to_response
from django.http import HttpResponse, HttpResponseRedirect, HttpResponseNotFound

from pbsmon.models import PBSJobs

import json

static_directory=os.path.join(os.path.dirname(__file__), 'static')

def index(request):
    pbs_jobs = PBSJobs.objects.all()
    return render_to_response('pbsmon/pbsmon.html', {'pbs_jobs': pbs_jobs})


def job_infos_from_PBSJobs(pbs_jobs):
    return [{'hostname': j.hostname,
             'username': j.username,
             'jobid': j.jobid,
             'jobname': j.jobname,
             'elapsed_time': j.elapsed_time} for j in pbs_jobs]

def get_state(request):
    pbs_jobs = PBSJobs.objects.all()
    job_infos_json = json.dumps(job_infos_from_PBSJobs(pbs_jobs))
    return HttpResponse(job_infos_json,
                        mimetype="application/json")
    

def PBSJob_from_job_info(job_info):
    return PBSJobs(hostname=job_info['hostname'],
                   username=job_info['username'],
                   jobid=job_info['jobid'],
                   jobname=job_info['jobname'],
                   elapsed_time=job_info['time'])

@csrf_exempt
def update(request):

    state = json.loads(request.POST['state'])

    hostname = state['hostname']

    old_pbs_jobs = PBSJobs.objects.filter(hostname=hostname)
    old_pbs_jobs.delete()

    job_infos = state['job_infos']
    for job_info in job_infos:    
        j = PBSJob_from_job_info(job_info)
        j.save()
    return HttpResponse('OK')


static_file_types = {'.js': 'text/javascript',
                     '.css' : 'text/css'}

def get_mimetype(filename):
    global static_file_types
    ext = os.path.splitext(filename)[-1]
    if ext in static_file_types:
        return static_file_types[ext]
    else:
        return None

def static(request, static_file):
    global static_directory
    static_files = os.listdir(static_directory)
    if static_file not in static_files:
        return HttpResponseNotFound("No such file.")
    with open(os.path.join(static_directory, static_file)) as f:
        return HttpResponse(f.read(),
                            mimetype=get_mimetype(static_file))
