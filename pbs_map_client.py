import imp
import sys
import time
import uuid
import socket
from socket import gethostname
import traceback 
import Queue
import random

import pbs_map as ppm
import pbs_util.pbs_map_classes as pmc

def main(argv=[]):
    print sys.path
    num_args = 3
    if len(argv) != num_args:
        print 'This program is not meant to be executed directly.'
        sys.exit(1)
        
    host_name, port, authkey = argv

    port = int(port)

    print 'Creating CLIENT'
    worker_name = str(uuid.uuid4()).split('-')[0]
    m = ppm.ClientMapManager(address=(host_name, port), authkey=authkey)
    m.connect()

    work_queue = m.get_work_queue()
    result_queue = m.get_result_queue()

    try:
        try:
            main_info = tuple(m.get_main_info())
        except (socket.error, IOError, EOFError):
            # Try to make sure we don't hammer the system
            time.sleep(random.randint(0, 60 * 10))
            main_info = tuple(m.get_main_info())

        sys_path, main_file_name, = main_info
        sys.path = sys_path
        client_mod = imp.load_source('__main', main_file_name)
        client_mod.name = '__main__'

        for cls in client_mod.__dict__.values():
            try:
                cls.__module__ = '__main__'
            except AttributeError:
                pass

        globals().update(client_mod.__dict__)

        try:
            worker_info = tuple(m.get_worker_info())
        except (socket.error, IOError, EOFError):
            # Try to make sure we don't hammer the system
            time.sleep(random.randint(0, 60 * 10))
            worker_info = tuple(m.get_worker_info())

        print 'worker_info tuple = ', worker_info
        print 'worker_info str = ', str(worker_info)


        WorkerClass, single_shot = worker_info

        print 'Starting worker'

        startup_args = tuple(m.get_startup_args())

        worker = WorkerClass(*startup_args)

        while True:
            # TODO: Strip jobid, and start a process which periodically tells the manager that this jobid is running
            print 'Getting next job...'
            try:
                task = work_queue.get(True, 60 * 10)
            except Queue.Empty:
                print 'Starving for work.'
                break

            print 'Got task ', task

            # print 'Got task: ', task
            # print "\n%s\n" % pickle.dumps(task)

            if not task.resumed:
                try:
                    result = pmc.TaskResult(worker(task.work), taskid=task.taskid, incomplete=False)
                    print 'calculated result ', result
                except ppm.IncompleteTask as e:
                    result = pmc.TaskResult(e.args[0], taskid=task.taskid, incomplete=True)
            else:
                try:
                    result = pmc.TaskResult(worker.resume_work(task.work), taskid=task.taskid, incomplete=False)
                    print 'calculated result ', result
                except ppm.IncompleteTask as e:
                    result = pmc.TaskResult(e.args[0], taskid=task.taskid, incomplete=True)

            result.worker_name = worker_name
            result_queue.put(result)

            print 'single_shot = ', single_shot
            if single_shot:
                print 'Exiting after single shot.'
                break
    except Exception as e:
        exc_type, value, tb = sys.exc_info()
        ftb = traceback.format_tb(tb)
        ftb = ['hostname = %s ' % gethostname()] + ftb
        print '\n'.join(ftb)
        result = pmc.TaskResult((exc_type, value, ftb), 
                            taskid=None,
                            exception=True)
        print 'exception result = ', str(result)

        # print "\n%s\n" % pickle.dumps(result)
        result_queue.put(result)
        raise

        
    

if __name__ == "__main__":
    main(sys.argv[1:])
