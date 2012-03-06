"""Identify primes using pbs_map."""

import pbs_map as ppm

from socket import gethostname

class PrimeWorker(ppm.Worker):
    
    def __init__(self, master_name):
        self.master_name = master_name
        self.hostname = gethostname() # record the compute node's hostname.

    def __call__(self, n):
        is_prime = True
        for m in xrange(2,n):
            if n % m == 0:
                is_prime = False
                break

        return (self.master_name, self.hostname, n, is_prime)
        

if __name__ == "__main__":
    for (master, node, n, is_prime) in sorted(ppm.pbs_map(PrimeWorker, range(1000, 10100), 
                                            startup_args=(gethostname(),), # send the master node login to the worker
                                            num_clients=100)):
        if is_prime:
            print '%d is prime. Computed by %s' % (n, (master, node))
        else:
            print '%d is composite. Computed by %s' % (n, (master, node))
