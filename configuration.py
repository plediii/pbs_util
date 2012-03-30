
import os
import pwd
import ConfigParser

# TODO: there is no reason for these to be exclusive to pbs_map
numnodes=1
numprocs=1
clients_per_pbs=1
queue=None
walltime=None

max_submissions=200

from_address=None
sendto_email_address=None

def get_user_name():
    return pwd.getpwuid(os.getuid())[0]

def config_email(configuration):
    global from_address, sendto_email_address

    # default to user@localhost
    sendto_email_address=get_user_name() + '@localhost'
    from_address=get_user_name() + '@localhost'

def config_pbs(configuration):
    global numnodes, numprocs, clients_per_pbs, queue, walltime, max_submissions

    section = 'PBSUTIL'  # configuration section for pyamber module.

    if configuration.has_section(section):
        if configuration.has_option(section, 'numnodes'):
            numnodes = configuration.get(section, 'numnodes')

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

    if not os.path.exists(config_file_name):
        return

    configuration = ConfigParser.SafeConfigParser()
    configuration.read(config_file_name)

    config_pbs(configuration)

    config_email(configuration)




default_config_file_name = os.getenv('HOME') + '/.pbs_util.ini'
config(default_config_file_name)

local_config_file_name = os.getcwd() + '/pbs_util.ini'
config(local_config_file_name)

default_email_config_file_name = os.getenv('HOME') + '/.send_email.ini'
config(default_email_config_file_name)

