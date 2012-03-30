import pbs_util.pbs_map as ppm

class PrimeWorker(ppm.Worker):
    
    def __call__(self, n):
        is_prime = True
        for m in xrange(2,n):
            if n % m == 0:
                is_prime = False
                break

        return (n, is_prime)
        

if __name__ == "__main__":
    for (n, is_prime) in sorted(ppm.pbs_map(PrimeWorker, range(1000, 10100), 
                                            num_clients=100)):
        if is_prime:
            print '%d is prime' % (n)
        else:
            print '%d is composite' % (n)
