"""pbs.py test suite."""

import pbs
import unittest
import os.path
import os
import subprocess

from submit_command import run_command_here


def file_contents(filename):
    file = open(filename)
    contents = file.read()
    file.close()
    return contents

def dump_to_file(filename, contents):
    file = open(filename, 'w')
    file.write(contents)
    file.close()

class HelloWorldCase(unittest.TestCase):
    """Set up a hello world script for testing with."""

    def setUp(self):
        temp_output_filename = self.temp_output_filename = os.path.realpath('.') + '/temp.out'
        self.pbs_script_filename = os.path.realpath('.') + '/test.pbs'

        dump_to_file(self.pbs_script_filename,
                     pbs.generic_script("""
echo "Hello, World!" > %(temp_output_filename)s
sleep 1
""" % locals()))

    def tearDown(self):  
        if os.path.exists(self.temp_output_filename):
            os.remove(self.temp_output_filename)
        os.remove(self.pbs_script_filename)

class Check_qstat(HelloWorldCase):
    """Check that pbs.qstat works."""

    def test_qstat_real(self):
        """pbs.qstat should return a non false result when given something actually submitted."""
        qsub_process = subprocess.Popen(["qsub", self.pbs_script_filename], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        qsub_output = qsub_process.communicate()[0]
        
        assert pbs.qstat(job_id=qsub_output.splitlines()[0].split('.')[0])

    def test_qstat_not_present(self):
        """pbs.qstat should return None when given a pbs id that doesn't actuallye exist."""
        self.assertRaises(pbs.PBSUtilError, pbs.qstat, '12345.notreal')

class Check_qsub(HelloWorldCase):
    """Check that pbs.qsub works."""

    def test_qsub(self):
        """pbs.qsub runs without error"""
        pbs.qsub(self.pbs_script_filename)

    def test_qsub_submits(self):
        """check that qsub successfully submits a script."""
        pbs_id = pbs.qsub(self.pbs_script_filename)
        assert pbs.qstat(job_id=pbs_id), "failed to find stats for %s which was just submitted." % pbs_id

class Check_wait(HelloWorldCase):
    """Check that pbs.qsub is capable of blocking while waiting for a pbs job to finish."""
    
    def test_wait(self):
        """pbs.qwait should wait for a pbs job to finish running."""
        if os.path.exists(self.temp_output_filename):
            os.remove(self.temp_output_filename)
        pbs_id = pbs.qsub(self.pbs_script_filename)
        pbs.qwait(pbs_id)
        os.system('ls > /dev/null') # This triggers the panfs file system to make the file appear.
        assert os.path.exists(self.temp_output_filename), "pbs.qwait returned, but the expected output does not yet exist."

    
if __name__ == "__main__":
    unittest.main()
