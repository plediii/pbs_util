
import pbs_util.pbs_map as ppm
import test_pbs_map as tpm

if __name__ == "__main__":
    for result in sorted(ppm.pbs_map(tpm.IdentityWorker, range(100),
                                     num_clients=10)):
        print result
