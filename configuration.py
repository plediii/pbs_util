
import os
import pwd
import ConfigParser

# TODO: there is no reason for these to be exclusive to pbs_map
pbs_map_numnodes=1
pbs_map_numprocs=1
pbs_map_clients_per_pbs=1
pbs_map_queue=None
pbs_map_walltime=None

max_submissions=200

from_address=None
sendto_email_address=None

def get_user_name():
    return pwd.getpwuid(os.getuid())[0]

def config_email(configuration):
    global from_address, sendto_email_address

    # default to user@rice.edu
    sendto_email_address=get_user_name() + '@rice.edu'
    from_address=get_user_name() + '@localhost'

    # Try to load requested username
    dropbox_section = 'DROPBOX'
    email_section='email'
    from_email_section='from_email'
    if configuration.has_section(dropbox_section):
        if configuration.has_option(dropbox_section, email_section):
            sendto_email_address=configuration.get(dropbox_section, email_section)

        if configuration.has_option(dropbox_section, from_email_section):
            from_address=configuration.get(dropbox_section, from_email_section)


def config_pbs(configuration):
    global pbs_map_numnodes, pbs_map_numprocs, pbs_map_clients_per_pbs, pbs_map_queue, pbs_map_walltime, max_submissions

    pbs_map_section = 'PBSMAP'  # configuration section for pyamber module.

    if configuration.has_section(pbs_map_section):
        if configuration.has_option(pbs_map_section, 'numnodes'):
            pbs_map_numnodes = configuration.get(pbs_map_section, 'numnodes')

        if configuration.has_option(pbs_map_section, 'numprocs'):
            pbs_map_numprocs = configuration.get(pbs_map_section, 'numprocs')

        pbs_map_clients_per_pbs= int(pbs_map_numnodes) * int(pbs_map_numprocs)


        if configuration.has_option(pbs_map_section, 'queue'):
            pbs_map_queue = configuration.get(pbs_map_section, 'queue')

        if configuration.has_option(pbs_map_section, 'walltime'):
            pbs_map_walltime = configuration.get(pbs_map_section, 'walltime')


        if configuration.has_option(pbs_map_section, 'max_submissions'):
            max_submissions = configuration.getint(pbs_map_section, 'max_submissions')


    

def config(config_file_name):
    """Configure pyamber using the config parser."""

    if not os.path.exists(config_file_name):
        return

    configuration = ConfigParser.SafeConfigParser()
    configuration.read(config_file_name)

    config_pbs(configuration)

    config_email(configuration)




default_config_file_name = os.getenv('HOME') + '/.pypbs.ini'
config(default_config_file_name)

local_config_file_name = os.getcwd() + '/pypbs.ini'
config(local_config_file_name)

default_email_config_file_name = os.getenv('HOME') + '/.send_email.ini'
config(default_email_config_file_name)

