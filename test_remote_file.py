
import os
import time

import pbs_map as ppm

import tempfile_util as tfu


class WriteFileWorker(ppm.Worker):

    def __call__(self, w):
        filename = tfu.temp_file_name("remotetest", local=True, small=True)
        
        with open(filename, 'w') as f:
            f.write("remote test %s" % (w))

        return filename

class ReadFileWorker(ppm.Worker):
    
    def __call__(self, w):
        filename = w
        
        with open(filename) as f:
            return f.read()

def main():

    with ppm.PBSMap(WriteFileWorker, (), num_clients=1) as write_session:
        with ppm.PBSMap(ReadFileWorker, (), num_clients=1) as read_session:
            for result in read_session.map(write_session.map(xrange(10))):
                print result

if __name__ == "__main__":
    main()
        
