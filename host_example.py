"""Identify primes using pbs_map."""

import pbs_util.pbs_map as ppm

from socket import gethostname

class PrimeWorker(ppm.Worker):
    
    def __init__(self, master_name):
        self.master_name = master_name
        self.hostname = gethostname() # record the compute node's hostname.

    def __call__(self, n):
        return (self.master_name, self.hostname)
        

if __name__ == "__main__":
    for (master, node) in ppm.pbs_map(PrimeWorker, range(1, 100), 
                                      startup_args=(gethostname(),), # send the master node login to the worker
                                      num_clients=100):
        print 'Received result from %s who received work from %s' % (node, master)
