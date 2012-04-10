import sys
import time
import os
import logging
import Queue
import inspect
import random
import multiprocessing.managers as mm
import uuid
from socket import gethostname

import pbs
from submit_command import run_command_here
import nice_submit as ns
import tempfile_util as tfu

import itertools as it

import configuration

import pbs_map_classes as pmc

import pbs_map_client 

class IncompleteTask(Exception):
    pass

class Worker(object):


    def __init__(self, *args, **kwargs):
        try:
            startup = self.startup
            print 'WARNING: Deprecated use of pbs_map.Worker.startup'
            return startup(*args, **kwargs)
        except AttributeError:
            pass

    def __call__(self, *args, **kwargs):
        try:
            do_work = self.do_work
            print 'WARNING: Deprecated use of pbs_map.Worker.do_work'
            return do_work(*args, **kwargs)
        except AttributeError:
            pass

    def resume_work(self, w):
        print 'WARNING: No protocol implemented for resume_work.'
        return self.__call__(w)

class MasterMapManager(mm.BaseManager):
    pass


def empty_queue(q):
    while not q.empty():
        q.get()


class TupleProxy(mm.BaseProxy):
    
    _exposed_=('__getitem__')

    def __getitem__(self, index):
        return self._callmethod('__getitem__')

class GeneratorProxy(mm.BaseProxy):
    _exposed_ = ('next', '__next__')
    def __iter__(self):
        return self
    def next(self):
        return self._callmethod('next')
    def __next__(self):
        return self._callmethod('__next__')

class ClientMapManager(mm.BaseManager):
    pass

ClientMapManager.register('get_work_queue')
ClientMapManager.register('get_result_queue')
ClientMapManager.register('get_main_info', proxytype=GeneratorProxy)
ClientMapManager.register('get_worker_info', proxytype=GeneratorProxy)
ClientMapManager.register('get_startup_args', proxytype=GeneratorProxy)

class PBSMapError(Exception):
    pass


def next_or_none(iterator):
    """Return the next item from the iterator, or else None."""
    try:
	return iterator.next()
    except StopIteration:
	return None


def remove_taskid(target_jobid, work_queue):
    """Remove any copies of jobid from the queue."""
    submitted_tasks = []
    try:
        for count in xrange(work_queue.qsize()):
            task = work_queue.get(block=False)
            if task is not None:
                if task.taskid == target_jobid:
                    continue
            submitted_tasks.append(task)
    except Queue.Empty:
        pass
    except ValueError:
        pass

    submitted_tasks.reverse()

    for job in submitted_tasks:
        work_queue.put(job)


def new_task_iterator(taskid_iterator, work_iterator):
    for taskid, work in it.izip(taskid_iterator, work_iterator):
        yield pmc.Task(work, taskid=taskid, resumed=False)

class OOPTaskLifo(object):

    def __init__(self, taskid_iterator):
        self.taskid_iterator = taskid_iterator
        self.oob_work = []

    def __len__(self):
        return len(self.oob_work)

    def empty(self):
        return self.oob_work == []

    def add_task(self, work, resumed=True):
        task = pmc.Task(work, taskid=self.taskid_iterator.next(), resumed=resumed)
        self.oob_work = [task] + self.oob_work
        return task.taskid

    def next_task(self):
        return self.oob_work[-1]

    def pop_next_task(self):
        return self.oob_work.pop()

class TaskManager(object):

    def __init__(self, work=None):
        self.submitted_jobs = 0
        self.received_jobs = 0
        self.finished_tasks = set()
        self.next_job = None
        self.submitted_jobs = 0
        self.submitted_tasks = {}
        self.results_since_restart = 0

        self.taskid_iterator = it.count(1)

        self.tasks_submitted = 0
        self.tasks_completed = 0
        

        if work:
            self.work_iterator = new_task_iterator(self.taskid_iterator, work)
            self.next_task = next_or_none(self.work_iterator)
        else:
            self.next_task = None

        self.oob_task_lifo = OOPTaskLifo(self.taskid_iterator)


    def new_work(self, work):
        self.work_iterator = new_task_iterator(self.taskid_iterator, work)
        self.next_task = next_or_none(self.work_iterator)


    def __check_invariant(self):
        assert self.tasks_submitted >= self.tasks_completed, '%s < %s' % (self.tasks_submitted, self.tasks_completed)
        assert len(self.submitted_tasks) == self.submitted_jobs - self.received_jobs, '%s != %s - %s = %s' % (len(self.submitted_tasks),
                                                                                                              self.submitted_jobs,
                                                                                                              self.received_jobs,
                                                                                                              self.submitted_jobs - self.received_jobs)
        assert len(self.finished_tasks) + len(self.submitted_tasks) == self.submitted_jobs

    def check_invariants(func):
        def new_func(self, *args, **kwargs):
            try:
                self.__check_invariant()
            except:
                print 'Pre function call invariant exception in %s' % func
                raise

            result = func(self, *args, **kwargs)
            try:
                self.__check_invariant()
            except:
                print 'Post function call invariant exception in %s' % func
                raise
            return result
        return new_func

    
    @check_invariants
    def has_job_to_submit(self, resubmit=False):
        self.__check_invariant()
        if self.next_task is not None:
            return True
        if not self.oob_task_lifo.empty():
            return True
        logging.info('attempt resubmit = %s, tasks left = %s' % (resubmit, len(self.submitted_tasks)))
        return resubmit and len(self.submitted_tasks) > 0
        
    @check_invariants
    def has_incomplete_tasks(self):
        # logging.info('next_job = %s, received_jobs = %s, submitted_jobs = %s' % (self.next_task, self.received_jobs, self.submitted_jobs))
        logging.info('tasks_submitted = %s, tasks_completed = %s' % (self.tasks_submitted, self.tasks_completed))
        return (self.next_task is not None) or (self.received_jobs < self.submitted_jobs) or (not self.oob_task_lifo.empty()) or (self.tasks_submitted > self.tasks_completed)
    
    @check_invariants
    def next_redundant_job(self):
        logging.info('pulling redundant job from %d previous submitted.' % (len(self.submitted_tasks)))
        assert len(self.submitted_tasks) > 0
        
        redundant_task = random.sample(self.submitted_tasks.values(), 1)[0]

        # logging.info('redundant task = %s' % repr(redundant_task))
        
        return redundant_task

    @check_invariants
    def submit(self, work_queue):
        assert self.has_job_to_submit(resubmit=True)

        try:                    # catch all the Queue.Full exceptions
            if not self.oob_task_lifo.empty():
                task = self.oob_task_lifo.next_task()
                # logging.info('attempting to submit a resumed task %s' % task)

                work_queue.put(task, block=False)                
                
                self.oob_task_lifo.pop_next_task()

                self.submitted_jobs += 1
                self.submitted_tasks[task.taskid] = task

            
            elif self.next_task is not None:
                task = self.next_task
                # logging.info('attempting to submit new task: %s' % task)

                work_queue.put(task, block=False)
                self.tasks_submitted += 1
                
                # logging.info('tasks_submitted: %s' % repr(self.next_task))

                self.next_task = next_or_none(self.work_iterator)

                self.submitted_jobs += 1
                self.submitted_tasks[task.taskid] = task

            else:
                task = self.next_redundant_job()
                # logging.info('attemping to resubmit task: %s' % task) 

                work_queue.put(task, block=False)

                logging.info('submitted_jobs = %s' % self.submitted_jobs)
            return True

        except Queue.Full:
            logging.info('work queue is full.')
            return False

    @check_invariants
    def collect_tasks(self, result_queue, work_queue,
                      patience=False):
        if patience:
            timeout=60
            logging.info('patiently collecting task')
        else:
            logging.info('impatiently collecting task')
            timeout=0

        try:
            task_result = result_queue.get(timeout=timeout)
        except Queue.Empty:
            logging.info('result queue is empty.')
            return False, None


        if task_result.exception:
            logging.warning('Worker raised an exception')
            exc_type, value, tb = task_result.result
            raise exc_type("%s\n%s" % (value, '\n'.join(tb)))

        # logging.info('Got task result: %s' % task_result)


        if task_result.incomplete:
            # job should be resumed.
            is_new = self.finish_task(task_result.taskid, work_queue)
            if is_new:
                logging.warning('new result to be resumed')
                self.oob_task_lifo.add_task(task_result.result)
            else:
                logging.warning('previously collected result to be resumed')
            return False, task_result.result
        else:
            # job is complete
            is_new = self.finish_task(task_result.taskid, work_queue)
            if is_new:
                logging.warning('new complete result: %s'% task_result.taskid)
                self.tasks_completed += 1
            else:
                logging.warning('previously collected complete result')
            # logging.info('task completed was %s' % (task_result))
            return is_new, task_result.result


    @check_invariants
    def finish_task(self, taskid, work_queue):
        assert taskid > 0

        remove_taskid(taskid, work_queue) # if we requeued this work, take it back off

        if taskid in self.submitted_tasks:
            self.submitted_tasks.pop(taskid)
        else:
            assert taskid in self.finished_tasks
        
        if taskid not in self.finished_tasks:
            self.finished_tasks.add(taskid)
            self.received_jobs += 1
            return True         # This is the first time we've seen this result

        return False            # We've seen it before
        

        

class PBSClientManager(object):

    def __init__(self, client_script_file_name, address, authkey, map_name, desired_number_of_clients):
        self.client_script_file_name = client_script_file_name
        self.address = address
        self.authkey = authkey
        self.map_name = map_name
        self.desired_number_of_clients = desired_number_of_clients
        self.dead_man_switch = False
        self.working_clients = set()
        self.clients_seen = set()

    def hit_dead_man_switch(self):
        if not self.dead_man_switch:
            self.clients_seen = set()
            self.working_clients = set(self.__clients())
            self.dead_man_switch = True

    def would_start_more_clients(self):
        return self.num_living_clients() < self.desired_number_of_clients

    def num_living_clients(self):
        return len(self.__clients())


    def __clients(self):
        """Return a list of the living pbs clients."""
        map_name = self.map_name
        client_jobs = []

        got_clients = False
        while not got_clients:
            try:
                for job in pbs.qstat(user=os.environ['USER']):
                    if job.name == map_name:
                        client_jobs.append(job)
                got_clients=True
            except pbs.PBSUtilQStatError, e:
                logging.warning('ERROR: Bad qstat output. %s' % e)
                time.sleep(5)
                got_clients=False
            
        return [job.id for job in client_jobs]


    def kill_clients(self):
        map_name = self.map_name
        for client in self.__clients():
            try:
                pbs.qdel(client)
            except OSError:
                pass

    def start_more_clients(self):
        if not self.would_start_more_clients():
            return

        # alternative, turn the switch using the client names
        logging.info('checking if clients are failing...')
        clients = set(self.__clients())
        if self.dead_man_switch:
            self.working_clients = set(clients)
            self.clients_seen = set()
            logging.info('ok because of dead man switch')
        else:
            if self.working_clients.isdisjoint(clients):
                logging.info('none of the current clients have returned work')
                # It is not possible that I've seen any living client do work.
                if len(self.clients_seen) < self.desired_number_of_clients * 2:
                    # I have seen more than the desired number of clients be created
                    logging.info('ok because have not seen too many clients: %d < %d' % (len(self.clients_seen), self.desired_number_of_clients * 2))
                    self.clients_seen = self.clients_seen.union(clients)
                elif self.clients_seen.isdisjoint(clients):
                    # If the first 2*desired number of clients that I started since I've seen work have died, then jobs are failing.
                    # logging.info('FAILING because we have seen too many clients, and none of them are alive: %s !~= %s' % (self.clients_seen, clients))
                    raise PBSMapError("All clients are failing.")
                # else:
                #     logging.info('we have seen too many clients, but some are alive: seen=%s still_alive = %s' % (len(self.clients_seen), clients.intersection(self.clients_seen)))
            # else:
                # logging.info('ok because working clients are alive: %s ~= %s' % (self.working_clients, clients))

        # logging.info('clients since switch (%d) %s.' % (len(self.clients_seen), self.clients_seen))

        self.dead_man_switch = False

        client_script_file_name = self.client_script_file_name
        num_clients = self.desired_number_of_clients
        map_name = self.map_name
        address = self.address
        authkey = self.authkey

        num_living = len(clients)
        num_clients_to_start = num_clients - num_living
        logging.info('Starting %d clients.' % num_clients_to_start)
        if num_clients_to_start <= 0:
            return
        
        client_script_file_names = [client_script_file_name for count in xrange(num_clients_to_start)]

        try:
            ns.submit_files_until_done(client_script_file_names, 
                                       quiet=True,
                                       fail_when_max=True,
                                       retry_on_failure=False)
            if self.working_clients.isdisjoint(clients):
                if len(self.clients_seen) < self.desired_number_of_clients * 2:
                    self.clients_seen = self.clients_seen.union(set(self.__clients()).difference(self.working_clients))
        except ns.NiceSubmitError as e:
            if isinstance(e, ns.ReachedMax):
                logging.warning('pbs submission limit reached.')
            elif isinstance(e, ns.QSubFailure):
                logging.warning('qsub failed to submit a job.')
            else:
                logging.warning('Some Nice submit failure: %s' % e)
            num_living = len(self.__clients())
            logging.info('num living = %s' % num_living)
            if num_living == 0:
                logging.warning('Number of living clients is zero.  Nothing to do.')
                time.sleep(10)


class PBSMap(object):

    pbs_map_exe=os.path.realpath(pbs_map_client.__file__)

    def __init__(self, cls, startup_args,
                 queue_timeout=None,
                 options=None,
                 single_shot=None,
                 num_clients=None):

        if num_clients is None:
            try:
                num_clients = options.num_clients
            except AttributeError:
                pass


            try:
                single_shot = options.single_shot
            except AttributeError:
                pass

        if num_clients is None:
            raise Exception("Number of clients to use has not been set.  This should be provided in the command line options or in num_clients kwarg for the pbs_map() function call.")

        map_name = str(uuid.uuid4()).split('-')[0]
        logging.basicConfig(filename='tmp_pbs_map_' + map_name + '.log', level=logging.INFO)

        main_file = get_main_file()
        logging.info('main_file = %s' % (main_file))
        worker_info = (cls,single_shot)


        self.cls = cls

        self.main_file = main_file
        self.worker_info = worker_info
        if queue_timeout is not None:
            self.queue_timeout=queue_timeout
        else:
            self.queue_timeout=120



        logging.info('Starting ' + map_name)

        master_work_queue = Queue.Queue(maxsize=max(1, (configuration.clients_per_pbs * num_clients)))
        master_result_queue = Queue.Queue(maxsize=max(1, (4 * configuration.clients_per_pbs * num_clients)))
	# master_running_queue = Queue.Queue()
        # master_work_queue.cancel_join_thread()
        # master_result_queue.cancel_join_thread()

        authkey = map_name

        MasterMapManager.register('get_work_queue', callable=lambda:master_work_queue)
        MasterMapManager.register('get_result_queue', callable=lambda:master_result_queue)

        def get_main_info():
            for x in (sys.path, self.main_file,):
                yield x
        def get_worker_info():
            for x in tuple(self.worker_info):
                yield x
        def get_startup_args():
            for x in startup_args:
                yield x

        MasterMapManager.register('get_main_info', get_main_info, proxytype=GeneratorProxy)
        MasterMapManager.register('get_worker_info', get_worker_info, proxytype=GeneratorProxy)
        MasterMapManager.register('get_startup_args', get_startup_args, proxytype=GeneratorProxy)
        self.manager = manager = MasterMapManager(address=('',0), authkey=authkey)
        manager.start()

        address = (gethostname(), manager.address[1])


        addr_repr = repr(('localhost', manager.address[1]))
        logging.info('Connecting locally to manager at %s' % addr_repr)
        m = ClientMapManager(address=('localhost', manager.address[1]), authkey=authkey)
        m.connect()

        logging.info('Getting local queues.')
        self.work_queue = m.get_work_queue()
        self.result_queue = m.get_result_queue()

        info_repr = repr(tuple(m.get_worker_info()))
        logging.info('worker_info = %s' % info_repr)

        self.tfs = tfu.Session(local=True, small=True)

        self.client_manager = None

        client_script_file_name = self.create_map_client_script(self.tfs, address, authkey, map_name)

        self.client_manager = self.create_client_manager(client_script_file_name, address, authkey, map_name,
                                                         num_clients)


        self.task_manager = self.create_task_manager()



    def create_map_client_script(self, tfs, host_address, authkey, map_name):
        pbs_map_exe=self.pbs_map_exe

        script_file_name = tfs.temp_file_name('.pbs')

        # It's not possible for the temp file system to delete files created on the nodes.
        if os.getenv('DELETE') == 'FALSE':
            output_file_name = script_file_name + '.out'
            err_output_file_name = script_file_name + '.err'
        else:
            output_file_name = '/dev/null'
            err_output_file_name = '/dev/null'
            

        if configuration.clients_per_pbs > 1:
            base_command = ["mpiexec", "python"]
        else:
            base_command = ["python"]

        hostname, port = host_address
        run_command_here(script_file_name, 
                         ' '.join(base_command + 
                                  [pbs_map_exe,
                                   hostname, 
                                   str(port), 
                                   authkey]), 
                         job_name=map_name,
                         output_file_name=output_file_name,
                         err_output_file_name=err_output_file_name,
                         numcpu=configuration.numprocs,
                         numnodes=configuration.numnodes,
                         queue=configuration.queue,
                         walltime=configuration.walltime,
                         mem=configuration.pmem,
                         disable_mpi=True)
        return script_file_name


    


    def put_more_jobs(self, task_manager, work_queue, resubmit=False):
        # Put jobs on the queue until we run out of jobs
        # or the queue is full

        num_put = 0
        while True:
            has_job = task_manager.has_job_to_submit(resubmit=resubmit)
            successful_submit = False
            logging.info('Attempting to put a new job: has_job = %s' % has_job)
            if has_job:
                successful_submit = task_manager.submit(work_queue)
                logging.info('Attempting to put a new job: success = %s' % successful_submit)
                if successful_submit:
                    num_put += 1

            if not has_job or not successful_submit:
                break


        if num_put > 0:
            logging.info('Put %d new jobs on the queue.' % num_put)
            # logging.info('%d tasks working' % len(task_manager.submitted_tasks))
            # logging.info('submitted_tasks: ' + str(task_manager.submitted_tasks.keys()[:20]))
        # else:
        #     logging.info('Did not put more jobs on the queue: %s' % repr(task_manager.next_job))

        return num_put
        

    def loop_while_results(self, task_manager, client_manager, result_queue, work_queue, patience=False):

        logging.info('result queue # = %s' % result_queue.qsize())
        logging.info('Receiving next result...')

        while True:
            a_result, result = task_manager.collect_tasks(result_queue, work_queue,
                                                          patience=patience)
            patience=False
            if a_result or result is not None:
                logging.info('Hitting dead man switch.')
                client_manager.hit_dead_man_switch()
            if not a_result:
                if result is None:
                    logging.info('not a result: %s. ' % result)
                return
            if hasattr(self.cls, 'post'):
                yield self.cls.post(result)
            else:
                yield result

    def create_task_manager(self, work=None):
        return TaskManager(work=work)

    def create_client_manager(self, client_script_file_name, address, authkey, map_name, desired_number_of_clients):
        return PBSClientManager(client_script_file_name, address, authkey, map_name, desired_number_of_clients)

    def loop_while_work(self, client_manager, task_manager, work_queue, result_queue):
        def resubmit():
            return result_queue.qsize() == 0

        while task_manager.has_incomplete_tasks():

            logging.info('work queue size = %s' % work_queue.qsize())

            if not work_queue.full():
                num_put = self.put_more_jobs(task_manager, work_queue, resubmit=resubmit())
            else:
                num_put = 0

            logging.info('new queued work = %s' % num_put)

            # TODO: how to detect if clients are dying without doing work?

            # Answer: count the total number of clients started,
            # resetting when a task is returned. If total number of
            # clients goes above twice the desired, and no tasks have
            # been returned, they are all dying.
            if client_manager.would_start_more_clients():
                client_manager.start_more_clients()

            num_results = 0

            # def patience():
            #     # If we aren't interested in starting more clients or
            #     # putting up more work, then just relax and wait for
            #     # results
            patience = (not client_manager.would_start_more_clients()) and (num_put <= 0)
            
            for result in self.loop_while_results(task_manager, client_manager, result_queue, work_queue,
                                                  patience=patience):
                # logging.info('result: %s' % repr(result))
                num_results += 1
                client_manager.hit_dead_man_switch()
                yield result



    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.client_manager is not None:
            self.client_manager.kill_clients()
        # have to close the proxy queues like this, otherwise the manager won't shut down
        # print 'Emptying queues...'
        # empty_queue(result_queue)
        # empty_queue(work_queue)

        # print result_queue.empty()

        # print work_queue.empty()

        logging.info('Closing queues')
        self.result_queue._close()
        self.work_queue._close()

        logging.info('Shutting down manager.')
        self.manager.shutdown()

        self.tfs.close()

        # except KeyboardInterrupt:
        #     exc_type, value, tb = sys.exc_info()
        #     print '\n'.join(traceback.format_tb(tb))
        #     print 'Waiting for TCP timers to finish... (this may take minutes).'
        #     raise


        return all(x is None for x in (exc_type, exc_value, traceback))

    def map(self, work):

	job_iterator = work

        self.task_manager.new_work(job_iterator)

        empty_queue(self.work_queue)
        empty_queue(self.result_queue)

        for result in self.loop_while_work(self.client_manager, self.task_manager, self.work_queue, self.result_queue):
            yield result

        empty_queue(self.work_queue)
        empty_queue(self.result_queue)
        

def partition_ranges(min_num, max_num, num_div):
    """Return num_div half open ranges the union of which is [0,num).

    Does not return the exact number of requrested partitions in some cases."""
    idc_divisions = range(min_num,max_num,int(round((max_num - min_num)/num_div)+1)) + [max_num]
    return zip(idc_divisions, idc_divisions[1:])
   

def pbs_map(cls, work, startup_args=(), **kwargs):
    with PBSMap(cls, startup_args, **kwargs) as map_session:
        for result in map_session.map(work):
            yield result

def pbs_mapcat(*args, **kwargs):
    for l in pbs_map(*args, **kwargs):
        for x in l:
            yield x

def add_option_parser_options(parser, num_clients=10):
    parser.add_option("--num-clients", dest="num_clients", default=num_clients, type="int",
                      help="Number of pbs jobs to submit (default %d)." % num_clients)
    parser.add_option("--single-shot", dest="single_shot", action="store_true", 
                      default=False,
                      help="Terminate each worker after a single task.")


def add_argparser_options(parser, num_clients=10):
    parser.add_argument("--num-clients", dest="num_clients", default=num_clients, type=int,
                        help="Number of pbs jobs to submit (default %d)." % num_clients)
    parser.add_argument("--single-shot", dest="single_shot", action="store_true", default=False,
                        help="Terminate each worker after a single task.")


def get_main_file():
    return os.path.abspath(sys.argv[0]) # This is the file actually being executed.
    

def parse_worker_info(cls):
    module_file_name = os.path.realpath(inspect.getsourcefile(cls))
    str_cls = str(cls)

    a = str_cls.rfind('.') + 1
    b = a + str_cls[a:].find("'")

    class_name = str_cls[a:b]

    return module_file_name, class_name

def get_startup_args(worker_info):
    return worker_info[2]

    
