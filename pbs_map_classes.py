

class Task(object):
    
    def __init__(self, work, taskid=None, resumed=False):
        self.taskid = taskid
        self.work = work
        self.resumed = resumed


    def __str__(self):
        if self.resumed:
            resume_label = 'Resumed task'
        else:
            resume_label = 'New Task'
        
        return "<%5s, %15s, '%s'>" % (self.taskid, resume_label, repr(self.work))

class TaskResult(object):
    
    def __init__(self, result, taskid=None, incomplete=False, exception=False):
        self.taskid = taskid
        self.result = result
        self.incomplete = incomplete
        self.exception = exception

    def __str__(self):
        if self.incomplete:
            incomplete_label = 'Incomplete task'
        else:
            incomplete_label = 'Complete Task'
        
        return "<%5s, %15s, '%s'>" % (self.taskid, incomplete_label, repr(self.result))
