#!/usr/bin/env python
#  Run the given command on a node

from __future__ import with_statement

import os
import sys
import optparse

import pbs
import nice_submit
import tempfile_util

import disable_mpi as disable_mpi_module

def run_command_here(script_file_name, command, input_file_name=None, output_file_name=None, err_output_file_name=None, job_name='submit_command', 
                     verbose=True, numcpu=None, mem=None, pmem=None, time=False, numnodes=None, queue=None, walltime=None,
                     disable_mpi=False):
    """Create a pbs script executing the given in the cwd command with given stdin and stderr/out files."""
    pwd = os.getcwd()

    if input_file_name:
        redirect_input = '< %s ' % input_file_name
    else:
        redirect_input = ''

    if time:
        command = 'time ' + command

    output_file_name = output_file_name or '/dev/null'
    if err_output_file_name is None:
        if output_file_name is not None:
            err_output_file_name = output_file_name + '.err'
        else:
            err_output_file_name = '/dev/null'

    if verbose:
        processor_verbose = """echo --------------------------------------
echo + Running on host `hostname`
echo + Time is `date` 
[ ${PBS_NODEFILE:-nonode} != "nonode" ] && echo + This job runs on the following processors: `cat ${PBS_NODEFILE}`
echo -------------------------------------- 
""" % locals()
    else:
        processor_verbose = ""

    additional_configuration_lines = []

    additional_configuration = '\n'.join(additional_configuration_lines)

    if disable_mpi:
        module_file = disable_mpi_module.__file__
        if module_file[-1] == 'c':
            module_file = module_file[:-1]
        disable_mpi = 'python ' + module_file
    else:
        disable_mpi = ''

    with open(script_file_name, 'w') as script_file:
        script =  pbs.generic_script("""

working_directory=%(pwd)s

if [ ! -d ${working_directory} ]; then
   echo "Can not execute, working directory does not exist: ${working_directory}"
   exit 1
fi


cd ${working_directory}

%(processor_verbose)s

%(disable_mpi)s

%(command)s %(redirect_input)s
""" % locals(),
                                     numnodes=numnodes,
                                     numcpu=numcpu,
                                     queue=queue,
                                     walltime=walltime,
                                     mem=mem, pmem=pmem,
                                     job_name=job_name)

        script_file.write(script)
    

def main(argv):
    parser = optparse.OptionParser()
    parser.add_option('-o', dest='output_file_name', default=None,
                      help="Destination file for the output. ")
    parser.add_option('-i', action='store', dest='input_file_name',
                      help="Write input file contents to command's stdin.")
    parser.add_option('-v', action='store_true', dest='verbose', default=False,
                      help="Display the script to be submitted.")
    parser.add_option('-q', action='store_true', dest='quiet', default=False,
                      help="Do not include information about processor nodes in the output, or the state of the job.")
    parser.add_option('-w', action='store', dest='script_name', default=None,
                      help="Save the script as this file name.")
    parser.add_option('-x', action='store_true', dest='dont_submit', default=False,
                      help="Don't submit the job.  This is really only useful when combined with '-w'.")

    parser.add_option('-W', action='store_true', dest='wait', default=False,
                      help="Wait for the job to finish, and display its output.")

    parser.add_option('-n', action='store', dest='job_name',  default=None,
                      help="PBS jobname")
    parser.add_option('-p', action='store', dest='numcpu', 
                      default=None,
                      help="Number of cpus to use.")
    parser.add_option('--num-nodes', action='store', dest='numnodes', 
                      default=None,
                      help="Number of nodes to use.")



    expect_another = False
    for idx, arg in enumerate(argv):
        if expect_another:
            expect_another = False
            continue
        if arg[0] == '-':
            if arg[-1] in ['o', 'i', 'w', 'n', 'p']:
                expect_another = True
            continue
        break

    if arg[0] == '-':
        # no command was given
        idx += 1

    (options, args) = parser.parse_args(argv[:idx])
    command_parts = argv[idx:]
    command = ' '.join(command_parts)


    with tempfile_util.Session(local=True) as session:
        # Pick a file to output to
        if options.output_file_name is not None:
            output_file_name = options.output_file_name
            wait = False
        elif options.script_name:
            output_file_name = options.script_name + '.out'
            session.add_name(output_file_name + '.err')
        else:
            output_file_name = session.temp_file_name('.submit_command.out')
            session.add_name(output_file_name + '.err')
            if options.script_name:
                print 'Outputting to ', output_file_name
            wait = True

        # Pick a script name
        if options.script_name is not None:
            script_file_name = options.script_name
        else:
            script_file_name = session.temp_file_name('.submit_command.pbs')

        job_id = None
        if options.job_name is not None:
            job_name = options.job_name
        elif output_file_name is not None:
            job_name = output_file_name
        else:
            job_name = filter(lambda x: x.isalpha(), command_parts[0])

        # Create the script
        run_command_here(script_file_name, command, input_file_name=options.input_file_name, output_file_name=output_file_name, job_name=job_name, verbose=not options.quiet, numcpu=options.numcpu, numnodes=options.numnodes)

        if options.verbose:
            with open(script_file_name) as script_file:
                print script_file.read()

        # Maybe submit it
        if options.script_name is not None:
            print 'Wrote %s.' % script_file_name


        if options.dont_submit:
            return
        else:
            try:
                if not options.quiet:
                    print 'Submitting...'
                job_id, = nice_submit.submit_files_until_done([script_file_name], wait_for_all=False,
                                                              quiet=options.quiet)

                # If they directed the output, just exit
                if not options.wait:
                    return

                if not options.quiet:
                    print 'Waiting on job ', job_id, ' to finish running.'
                pbs.qwait(job_id)

            except KeyboardInterrupt: 
                print '^C'
                if job_id: 
                    print 'Killing ', job_id
                    pbs.qdel(job_id)

            if os.path.exists(output_file_name):
                with open(output_file_name) as output_file:
                    print output_file.read()
            else:
                print "Output file doesn't seem to exist! Try:\ncat %s\n" % output_file_name
            if os.path.exists(output_file_name + '.err'):
                with open(output_file_name + '.err') as err_file:
                    print err_file.read()



if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except KeyboardInterrupt:
        # I prefer to simply print ^C above than dump out this error here.
        pass
