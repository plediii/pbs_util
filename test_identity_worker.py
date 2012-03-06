
import pbs_map as ppm
import test_pbs_map as tpm

if __name__ == "__main__":
    for result in sorted(ppm.pbs_map(tpm.IdentityWorker, range(100))):
        print result
