"""pbs_map test suite."""


import unittest
import random
import sys

import pbs_map as ppm

print """

Note: If jobs are accepted to by the batch system but not executed, this test suite will hang. 

"""
# TODO: Use showstart to identify hanging submissions

class IdentityWorker(ppm.Worker):

    def do_work(self, x):
	return x

class ResumedWork(object):

    def __init__(self, x):
        self.num_times_resumed = 0
        self.x = x

    def is_finished(self):
        return self.num_times_resumed == 3

    def next_step(self):
        self.num_times_resumed += 1
        return self

class ResumingIdentityWorker(IdentityWorker):

    def resume_work(self, x):
        if not x.is_finished():
            raise ppm.IncompleteTask(x.next_step())
        else:
            return super(ResumingIdentityWorker, self).do_work(x.x)

    def do_work(self, x):
        return self.resume_work(ResumedWork(x))
            
        

class SingleShotIdentityWorker(ppm.Worker):
    """Raise an exception if used more than once.

    The pbs map client should use this worker for one unit, and then
    exit.
    
    """

    def startup(self):
        self.first_shot = True

    def do_work(self, x):
        if self.first_shot:
            self.first_shot = False
            return x
        else:
            raise SingleShotError()

class ResumingSingleShotIdentityWorker(SingleShotIdentityWorker, ResumingIdentityWorker):
    pass


class RepeatListWorker(ppm.Worker):
    """Given an x, return the singleton list containing x."""

    def startup(self, num_times):
        self.num_times=num_times

    def do_work(self, x):
        return [x] * self.num_times

class SingletonListWorker(RepeatListWorker):
    """Given an x, return the singleton list containing x."""

    def startup(self):
        self.num_times=1

class SingleShotError(Exception):
    pass

def run_pbs_map(*args, **kwargs):
    return list(ppm.pbs_map(*args, **kwargs))


def run_pbs_mapcat(*args, **kwargs):
    return list(ppm.pbs_mapcat(*args, **kwargs))


class PBSMapCase(object):

    def run_pbs_map(self, *args, **kwargs):
        return run_pbs_map(*args, **kwargs)

def single_iterators(iterator):
    for x in iterator:
        def single_iterator():
            yield x
        yield single_iterator()
    


class ReusedPBSMapCase(PBSMapCase):
    
    def run_pbs_map(self, cls, work, **kwargs):
        if 'startup_args' in kwargs and kwargs['startup_args']:
            startup_args = kwargs['startup_args']
        else:
            startup_args = tuple()
        results = []
        with ppm.PBSMap(cls, startup_args, **kwargs) as mapper:
            for single_work in work:
                for result in mapper.map([single_work]):
                    results.append(result)

        return results



class PBSMapCatCase(PBSMapCase):

    def run_pbs_map(self, *args, **kwargs):
        return run_pbs_mapcat(*args, **kwargs)

    run_pbs_mapcat=run_pbs_map



class SingleShotPBSMapCase(object):

    def run_pbs_map(self, *args, **kwargs):
        kwargs['single_shot'] = True
        return run_pbs_map(*args, **kwargs)


class RangeCase(object):

    max_range=100
    WorkerClass=IdentityWorker

    def test_pbs_map(self):

	xs = range(self.max_range)

	results = self.run_pbs_map(self.WorkerClass, xs, queue_timeout=5, num_clients=10)

	self.assertEqual(sorted(results), sorted(xs))

class ResumingRangeCase(RangeCase):
    
    WorkerClass=ResumingIdentityWorker


class MapRangeCase(PBSMapCase, RangeCase):
    pass

class MapCatRangeCase(PBSMapCatCase, RangeCase):
    WorkerClass=SingletonListWorker

class RepeatRangeCase(object):
    """Run pbs map over a range list, expecting multiple copies of
    each element to be returned."""

    max_range=100
    WorkerClass=RepeatListWorker

    def test_pbs_mapcat(self):
        
        xs = range(self.max_range)

        num_times = random.randint(1,5)

	results = self.run_pbs_mapcat(self.WorkerClass, xs, queue_timeout=5, num_clients=10, startup_args=(num_times,))

	self.assertEqual(sorted(results), sorted(xs * num_times))
        

class MapCatRepeatRangeTestCase(RepeatRangeCase, PBSMapCatCase, unittest.TestCase):
    pass


class SingleShotFailureCase(PBSMapCase):
    """By submitting enough jobs to guarantee each client will see at
    least two, and using PBSMapCase instead of SingleShotPBSMapCase,
    we ensure that a single shot worker should raise an exception.


    This tests both our ability to catch worker exceptions, and the
    expected behavior of the singleshot worker.
    
    """

    WorkerClass=SingleShotIdentityWorker
    
    max_range=100

    def test_single_shot_failure(self):
        
        num_clients=10
        assert self.max_range > num_clients
        xs = range(self.max_range)

        try:
            results = self.run_pbs_map(self.WorkerClass, xs, queue_timeout=5, num_clients=num_clients)
            assert False, "Single shot error not raised.  results = %s" % results
        except Exception, e:
            # There are some weird semantics on the type of the exception depending on how this test is called.
            self.assertTrue(str(e).find('SingleShotError') > 0, 'did not receive a SingleShotError: %s' % str(e) )


class SingleShotSuccessCase(SingleShotPBSMapCase):

    WorkerClass=SingleShotIdentityWorker

    max_range=100

    def test_single_shot_success(self):

        num_clients=10
        assert self.max_range > num_clients
        xs = range(self.max_range)

        try:
            self.run_pbs_map(self.WorkerClass, xs, queue_timeout=5, num_clients=num_clients)
        except SingleShotError:
            assert False, "Single shot error was improperly raised."

        
class SingleShotFailureTestCase(SingleShotFailureCase, unittest.TestCase):
    pass

class SingleShotSuccessTestCase(SingleShotSuccessCase, unittest.TestCase):
    pass


class SingleShotMapIdentityTestCase(SingleShotPBSMapCase, MapRangeCase, unittest.TestCase):
    pass


class FailingWorkerMixin(object):
    """Simulate processor faillure by wrapping a worker and randomly
    sys.exiting instead of doing the work."""

    failure_probability = 0.05

    def do_work(self, x):
	if random.random() <= self.failure_probability:
	    sys.exit(1)
	else:
	    return super(FailingWorkerMixin, self).do_work(x)


class FailingIdentityWorker(FailingWorkerMixin, IdentityWorker):
    pass

class ResumingFailingIdentityWorker(ResumingIdentityWorker, FailingWorkerMixin, IdentityWorker):
    pass


class MapFailingIdentityTestCase(MapRangeCase, unittest.TestCase):
    WorkerClass=FailingIdentityWorker


class SingleShotMapFailingIdentityTestCase(SingleShotPBSMapCase, MapRangeCase, unittest.TestCase):
    WorkerClass=FailingIdentityWorker


class CompleteFailureWorkerMixin(FailingWorkerMixin):
    # failure_probability = 0.
    failure_probability = 1.

    # def test_complete_failure_broken(self):
    #     # Complete failure tests should have a failure_probability = 1.
    #     # Presently however, a complete failure causes the system to go
    #     # into an infinite loop, preventing over thetst from being run.  

    #     self.assertTrue(failure_probability < 1., "Unable to detect complete failures.")

class CompleteFailureIdentityWorker(CompleteFailureWorkerMixin, IdentityWorker):
    pass

class ResumingCompleteFailureIdentityWorker(ResumingIdentityWorker, CompleteFailureIdentityWorker):
    pass


class MapFailureCase(PBSMapCase):

    WorkerClass=CompleteFailureIdentityWorker

    max_range=10


    def test_pbs_map(self):

	xs = range(self.max_range)
        try:
            # This isn't clear.  Am I trying to catch a PBSMapError, or a TestException?
            # Depending on how I call python, both are possible
            self.assertRaises(ppm.PBSMapError, self.run_pbs_map, self.WorkerClass, xs, queue_timeout=5, num_clients=10)
        except Exception, e:
            self.assertTrue(isinstance(e, TestException))

class MapCompleteFailureIdentityTestCase(MapFailureCase, unittest.TestCase):
    pass

class TestException(Exception):
    pass

class ExceptionRaisingWorkerMixin(object):
    """Randomly raise an exception instead of doing work."""

    failure_probability = 0.05

    def do_work(self, x):
	if random.random() <= self.failure_probability:
            raise TestException()
	else:
	    return super(ExceptionRaisingWorkerMixin, self).do_work(x)


class ExceptionRaisingIdentityWorker(ExceptionRaisingWorkerMixin, IdentityWorker):
    pass


class MapExceptionCase(PBSMapCase):
    
    WorkerClass=ExceptionRaisingIdentityWorker
    max_range=100
    
    def test_raise_exception(self):
        
	xs = range(self.max_range)

	try:
	    self.run_pbs_map(self.WorkerClass, xs, queue_timeout=5, num_clients=10)
	    raise Exception("TestException was not raised.")
	except Exception as e:
	    self.assertEqual(ppm.parse_worker_info(e.__class__), ppm.parse_worker_info(TestException().__class__))

class MapExceptionTestCase(MapExceptionCase, unittest.TestCase):
    pass

class SingleShotMapExceptionTestCase(SingleShotPBSMapCase, MapExceptionCase, unittest.TestCase):
    pass



# Resume cases

class ResumedSingleShotFailureTestCase(SingleShotFailureTestCase, unittest.TestCase):
    WorkerClass=ResumingSingleShotIdentityWorker
    max_range=20

class ResumedSingleShotSuccessTestCase(SingleShotSuccessTestCase):
    WorkerClass=ResumingSingleShotIdentityWorker
    max_range=20


class ResumedSingleShotMapIdentityTestCase(SingleShotMapIdentityTestCase):
    WorkerClass=ResumingSingleShotIdentityWorker
    max_range=20


class ResumedMapFailingIdentityTestCase(MapFailingIdentityTestCase):
    WorkerClass=ResumingFailingIdentityWorker
    max_range=20

class ResumedSingleShotMapFailingIdentityTestCase(SingleShotMapFailingIdentityTestCase):
    WorkerClass=ResumingFailingIdentityWorker
    max_range=20    

class ResumedMapCompleteFailureIdentityTestCase(MapCompleteFailureIdentityTestCase):
    WorkerClass=ResumingCompleteFailureIdentityWorker
    max_range=20


class PBSMapTestCase(RangeCase, PBSMapCase, unittest.TestCase):
    pass


class ReusedPBSMapTestCase(RangeCase, ReusedPBSMapCase, unittest.TestCase):
    pass


if __name__ == "__main__":
    unittest.main()
