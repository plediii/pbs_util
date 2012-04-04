
from django.db import models

class PBSJobs(models.Model):
    hostname = models.CharField(max_length=50)
    username = models.CharField(max_length=50)
    jobid = models.CharField(max_length=50)
    jobname = models.CharField(max_length=50)
    elapsed_time = models.CharField(max_length=50)
    
