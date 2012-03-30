
# pbs_util
 
The `pbs_util` provides utility scripts and python modules for working
with the Portable Batch System.

This is not a wrapper for the C API, but includes a simple wrapper
around the PBS shell commands `qsub`, `qdel` and `qstat`.  

`pbs_util` provides a few command line tools and a module for
automatically running python functions on compute nodes.  For example,
the following is a complete program to identify prime numbers in
parallel on 100 nodes of a compute cluster:

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




## Dependencies

`pbs_util` depends on my `tempfile_util` module. 

## Configuration

Default options for qsub submission are set in either ~/.pbs_util.ini, or
pbs_util.ini in the local directory.  The ini file dictates, for example,
the number of nodes to request in a single job, the number of
processors on each node, the queue to submit to, and the maximum
number of simultaneous jobs to submit.

Here is an example pbs_util.ini I use:

     [PBSMAP]
     numnodes=1
     numprocs=1
     queue=serial
     max_submissions=624


## Testing

`pbs_util` includes a test suite to check its ability to submit jobs:

   python test_pbs_util.py

## Shell Utilities

### qdel_all

`qdel_all` kills all jobs submitted by the user.

### qdel_name

`qdel_name` kills all jobs with names matching the command line argument. 

For example, suppose jobs with names hello1, hello2 and world5 are
running or submitted to the queue.  Running `qdel_name hello` will
kill both hello1 and hello2, leaving world5 running.


### qdel_range

`qdel_range` kills all jobs with job id in the contiguous range specified by its arguments.  

For example, `qdel_range 433700 433705` will kill all jobs 433700..433705 inclusive.

### nice_submit

`nice_submit` takes a list of scripts to submit, and, running as a
daemon, submits the jobs consecutively until they are finished.  This
utility is useful when the number of jobs to submit is significantly
greater than the maximum number of simultaneous jobs allowed by the
cluster.  nice_submit will submit as many jobs as allowed and then
wait until the submitted jobs have completed before submitted more.  A
typical invocation is `nice_submit script_list`.

### pbs_chain

`pbs_chain` reads qsub job submission statements from stdin, and waits
for those jobs to complete before terminating; thus allowing chaining
dependent jobs together at the command line. 

As a trivial example, suppose I have a foo.pbs script, and a bar.pbs
script, where bar must be run after foo has completed.  These jobs can
be sequenced via:

   qsub hello.pbs | pbs_chain && qsub world.pbs | pbs_chain && echo "Done."

pbs_chain is robust to program noise, requring only that qsub
notifications appear at the beginning of a line.  As a more common use
case, I may have a large set of jobs to run which generate data, and a
final job which can analyze the results after they have completed.
pbs_chain can be combined with nice_submit at the command line in the
following way:

	nice_submit generate_scripts | pbs_chain && qsub analyze.pbs


### submit_command

submit_command makes one-liner job submission trivial.  Often I will
want to perform some computationally non-trivial task which should not
be performed on the login nodes, but am too lazy to set up the entire
PBS context to submit it.  Providing the command to submit_command
manages script creation and submission. 

As a trivial example, suppose we want to gzip foo.db, which is a huge
file in the current directory.  Instead of zipping the file on login
node, we can submit it to a compute node simply by:

      submit_command gzip foo.db

The above command will create a random script name and submit the job.
If we want a particular name for the batch script, we can provide it
via the "-w" comand line flag:

      submit_command -w gzip.pbs gzip foo.db

If we want the script, but don't want the job submitted, we can add
the "-x" command line option.  Finally, if we would like
submit_command to wait until the job finishes, and then dump the
result to the console (basically emulating an interactive run), we can
use the "-W" option.  For instance, the following will run `ls' in the
current directory on a remote compute node, but print the result to
stdout.

    submit_command -W ls


## Module Functions

### pbs_map

The collection of command line utilities in `pbs_util` are useful for
gluing together non-trivial PBS jobs at the command line; however they
can not address how to divide a large set of small jobs in an optimal
way to the compute nodes.  There is often a non-neglible delay between
requesting a script to be submitted and the actual invocation of the
script on a remote node.  The usual solution to this problem is to
merge several smaller jobs into a smaller set of macro jobs.  The
finite wall time on the cluster tends to mar the simplicity of this
approach.  

The purpose of pbs_map is to simplify both the way that jobs are to be
divided between nodes, and eliminate the tedium in manual submission
of multiple interdependent jobs.  pbs_map takes Worker class which
acts as a function, and an iterator of arguments to the Worker
function.  The workers are instantiated on the nodes and called on the
work arguments transmitted from the master node.  pbs_map guarantees
that a result from each work unit is collected (in no particular
order).

To demonstrate how pbs_map works, the following program will compute
the primality of integers in parallel on the compute nodes.

    import pbs_map as ppm

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


It is also possible to provide initialization arguments to the worker
class.  The following program displays on which hosts the client
programs are running.

    import pbs_map as ppm

    from socket import gethostname

    class HostNameWorker(ppm.Worker):

        def __init__(self, master_name):
            self.master_name = master_name
            self.hostname = gethostname() # record the compute node's hostname.

        def __call__(self, n):
            return (self.master_name, self.hostname)


    if __name__ == "__main__":
        for (master, node) in ppm.pbs_map(HostNameWorker, range(1, 100), 
                                          startup_args=(gethostname(),), # send the master node login to the worker
                                          num_clients=100):
            print 'Received result from %s who received work from %s' % (node, master)

