
import sys
import uuid


import pbs_map
import configuration


def new_worker_name():
    return str(uuid.uuid4()).split('-')[0]


def test_worker_names():
    worker_names = set()
    test_count=100*configuration.clients_per_pbs
    for count in xrange(test_count):
        worker_names.add(new_worker_name())

    return test_count==len(worker_names)
        

assert test_worker_names()

class MapCountWorker(pbs_map.Worker):

    def startup(self):
        self.name=None

    def do_work(self, w):
        name = w 

 #       time.sleep(60.0)

        if self.name is None:
            self.name = name
            return name
        else:
            sys.exit(1)
        # else:
        #     time.sleep(60.)

#        return self.name
    

def random_names():
    while True:
        yield new_worker_name()

def main(argv=[]):

    worker_names = set()

    print 'Waiting for %d workers...' % configuration.clients_per_pbs

    for worker_name in pbs_map.pbs_map(MapCountWorker, random_names(), num_clients=1):
        worker_names.add(worker_name)
        print len(worker_names)
        if len(worker_names) == configuration.clients_per_pbs:
            print 'Good.'
            break


if __name__ == "__main__":
    main(sys.argv[1:])
