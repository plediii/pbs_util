
import sys
import os
import pwd
import ConfigParser


numnodes=1
numprocs=1
clients_per_pbs=1
queue=None
walltime=None
pmem=None
mem=None

max_submissions=200

def get_user_name():
    return pwd.getpwuid(os.getuid())[0]

sendto_email_address=get_user_name() + '@localhost'
from_address=get_user_name() + '@localhost'

def config_email(configuration):
    global from_address, sendto_email_address

    section = 'PBSUTIL' 

    # default to user@localhost
    if configuration.has_section(section):
        if configuration.has_option(section, 'sendto'):
            sendto_email_address=configuration.get(section, 'sendto')

        if configuration.has_option(section, 'sendfrom'):
            from_email_address=configuration.get(section, 'sendfrom')


def config_pbs(configuration):
    global numnodes, numprocs, clients_per_pbs, queue, walltime, max_submissions, pmem, mem

    section = 'PBSUTIL' 

    if configuration.has_section(section):
        if configuration.has_option(section, 'numnodes'):
            numnodes = configuration.get(section, 'numnodes')


        if configuration.has_option(section, 'pmem'):
            pmem = configuration.get(section, 'pmem')


        if configuration.has_option(section, 'mem'):
            mem = configuration.get(section, 'mem')

        if configuration.has_option(section, 'numprocs'):
            numprocs = configuration.get(section, 'numprocs')

        clients_per_pbs= int(numnodes) * int(numprocs)


        if configuration.has_option(section, 'queue'):
            queue = configuration.get(section, 'queue')

        if configuration.has_option(section, 'walltime'):
            walltime = configuration.get(section, 'walltime')


        if configuration.has_option(section, 'max_submissions'):
            max_submissions = configuration.getint(section, 'max_submissions')

    

def config(config_file_name):
    """Configure pyamber using the config parser."""
    global some_config_file_exists

    if not os.path.exists(config_file_name):
        return

    some_config_file_exists = True

    configuration = ConfigParser.SafeConfigParser()
    configuration.read(config_file_name)

    config_pbs(configuration)

    config_email(configuration)



some_config_file_exists = False

default_config_file_name = os.getenv('HOME') + '/.pbs_util.ini'
config(default_config_file_name)

local_config_file_name = os.getcwd() + '/pbs_util.ini'
config(local_config_file_name)

default_email_config_file_name = os.getenv('HOME') + '/.send_email.ini'
config(default_email_config_file_name)

if not some_config_file_exists:
    sys.stderr.write("WARNING: Neither ~/.pyp_util.ini nor ./pbs_util.ini exist.")
