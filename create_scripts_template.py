#!/bin/env/python

import glob
import sys
import os
import optparse


import submit_command as sc

import coord_util.trajectory_database as trajdb



#gnat_population_histogram_exe = os.path.split(os.path.abspath(gnat_population_histograms.__file__))[0] + '/gnat_population_histograms.py'
gnat_population_histogram_exe = os.path.split(__file__)[0] + '/histogram_over_gnat_centers.py'

def partition_ranges(min_num, max_num, num_div):
    """Return num_div half open ranges the union of which is [0,num).

    Does not return the exact number of requrested partitions in some cases."""
    idc_divisions = range(min_num,max_num,int(round((max_num - min_num)/num_div)+1)) + [max_num]
    return zip(idc_divisions, idc_divisions[1:])


class CreateScriptsProgramTemplate(object):

    prog_args = []
    prog_usage = """ """

    def script_list_file_name(self, options, args):
        return "submit.lst"

    def num_procs(self, options, args):
        return 1

    def add_option_parser_options(self, parser):
        pass

    def scripts_and_commands(self, options, args):
        raise NotImplementedError("scripts_and_commands must be overridden in subclass.")
    
    def run(self, argv):
        subclass_args = ' '.join(self.prog_args)
        prog_usage = self.prog_usage

        parser = optparse.OptionParser("""%prog """ + """%(subclass_args)s 

%(prog_usage)s""" % locals())

        parser.add_option("--time", action="store_true", default=False, dest="time")

        self.add_option_parser_options(parser)

        options, args = parser.parse_args(argv)

        num_args = len(self.prog_args)

        if len(args) < num_args:
            parser.error("Required arguments were not provided.")
        elif len(args) > num_args:
            parser.error("Too many arguments were not provided.")

        num_procs = self.num_procs(options, args)

        script_list_file_name = self.script_list_file_name(options, args)

        with open(script_list_file_name, 'w') as script_list_file:
            for script_name, command in self.scripts_and_commands(options, args):
                sc.run_command_here(script_name, command, job_name=script_name, output_file_name=script_name + '.out', numcpu=str(num_procs), time=options.time)
                script_list_file.write(script_name + '\n')
        print 'Wrote %s.' % script_list_file_name
