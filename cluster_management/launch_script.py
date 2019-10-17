# TODO:
# import settings in an intelligent manner from some json config files
# code that validates and prints all parameters in the json files for review?
# write up a readme on how to use this script
# make a requirements file for the launch script
# python version? make it 2 because pycharm only lets you pick one.

# uh-oh - SSL settings - I guess we need to validate route53 DNS settings.

# major script components
# deploy new cluster
# update frontend?
# update data processing?
# stop data processing?
# deploy N data processing servers
# modify EB scaling settings?
# update rabbitmq/celery server

import argparse
import json
import os
import re
import sys
from os.path import relpath, join as path_join
from time import sleep

from fabric.api import put, run, sudo, env as fabric_env

from deployment_helpers.aws.elastic_beanstalk import (create_eb_environment,
    check_if_eb_environment_exists, fix_deploy)
from deployment_helpers.aws.elastic_compute_cloud import\
    (get_manager_instance_by_eb_environment_name, create_processing_control_server,
    create_processing_server, get_manager_public_ip, get_manager_private_ip)
from deployment_helpers.aws.iam import iam_purge_instance_profiles
from deployment_helpers.aws.rds import create_new_rds_instance
from deployment_helpers.configuration_utils import (
    are_aws_credentials_present, is_global_configuration_valid,
    reference_environment_configuration_file, reference_data_processing_server_configuration,
    validate_beiwe_environment_config, create_finalized_configuration,
    create_processing_server_configuration_file, create_rabbit_mq_password, get_rabbit_mq_password)
from deployment_helpers.constants import (
    APT_MANAGER_INSTALLS, APT_SINGLE_SERVER_AMI_INSTALLS, APT_WORKER_INSTALLS,
    DEPLOYMENT_ENVIRON_SETTING_REMOTE_FILE_PATH, DO_CREATE_ENVIRONMENT, DO_SETUP_EB_UPDATE_OPEN,
    ENVIRONMENT_NAME_RESTRICTIONS, EXTANT_ENVIRONMENT_PROMPT, FILES_TO_PUSH,
    get_beiwe_python_environment_variables_file_path, get_finalized_environment_variables,
    get_global_config, get_pushed_full_processing_server_env_file_path,
    get_server_configuration_file, get_server_configuration_file_path, HELP_SETUP_NEW_ENVIRONMENT,
    LOCAL_AMI_ENV_CONFIG_FILE_PATH, LOCAL_APACHE_CONFIG_FILE_PATH, LOCAL_CRONJOB_MANAGER_FILE_PATH,
    LOCAL_CRONJOB_SINGLE_SERVER_AMI_FILE_PATH, LOCAL_CRONJOB_WORKER_FILE_PATH,
    LOCAL_INSTALL_CELERY_WORKER, LOCAL_PYENV_INSTALLER_FILE, LOG_FILE, PUSHED_FILES_FOLDER,
    REMOTE_APACHE_CONFIG_FILE_PATH, REMOTE_CRONJOB_FILE_PATH, REMOTE_HOME_DIR,
    REMOTE_INSTALL_CELERY_WORKER, REMOTE_PYENV_INSTALLER_FILE, REMOTE_USERNAME, STAGED_FILES,
    USER_SPECIFIC_CONFIG_FOLDER, RABBIT_MQ_PORT, LOCAL_RABBIT_MQ_CONFIG_FILE_PATH,
    REMOTE_RABBIT_MQ_CONFIG_FILE_PATH, REMOTE_RABBIT_MQ_FINAL_CONFIG_FILE_PATH)
from deployment_helpers.general_utils import log, EXIT, current_time_string, do_zip_reduction, retry


# Fabric configuration
class FabricExecutionError(Exception): pass
fabric_env.abort_exception = FabricExecutionError
fabric_env.abort_on_prompts = True

####################################################################################################
################################### Fabric Operations ##############################################
####################################################################################################


def configure_fabric(eb_environment_name, ip_address, key_filename=None):
    if eb_environment_name is not None:
        get_finalized_environment_variables(eb_environment_name)
    if key_filename is None:
        key_filename = get_global_config()['DEPLOYMENT_KEY_FILE_PATH']
    fabric_env.host_string = ip_address
    fabric_env.user = REMOTE_USERNAME
    fabric_env.key_filename = key_filename
    retry(run, "# waiting for ssh to be connectable...")
    run("echo >> {log}".format(log=LOG_FILE))
    sudo("chmod 666 {log}".format(log=LOG_FILE))


def remove_unneeded_ssh_keys():
    """ This is based on a recommendation from AWS documentation:
    https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/
    building-shared-amis.html?icmpid=docs_ec2_console#remove-ssh-host-key-pairs
    Once you run this, you can't SSH into the server until you've rebooted it. """
    sudo("shred -u /etc/ssh/*_key /etc/ssh/*_key.pub")


def push_manager_private_ip_and_password(eb_environment_name):
    ip = get_manager_private_ip(eb_environment_name) + ":" + str(RABBIT_MQ_PORT)
    password = get_rabbit_mq_password()
    filename = path_join(REMOTE_HOME_DIR, "manager_ip")
    # echo puts a new line at the end of the output
    run("echo {text} > {filename}".format(text=ip, filename=filename))
    run("printf {text} >> {filename}".format(text=password, filename=filename))
    # command = "printf {text} > {filename}".format(
    #         text=ip + "\n" + password,
    #         filename=path_join(REMOTE_HOME_DIR, "manager_ip"))
    # run(command)
    
    
def push_files():
    for local_relative_file, remote_relative_file in FILES_TO_PUSH:
        local_file = path_join(PUSHED_FILES_FOLDER, local_relative_file)
        remote_file = path_join(REMOTE_HOME_DIR, remote_relative_file)
        put(local_file, remote_file)


def load_git_repo():
    """ Get a local copy of the git repository """
    # Git clone the repository into the remote beiwe-backend folder
    # Note that here stderr is redirected to the log file, because git clone prints
    # to stderr rather than stdout.
    run('cd {home}; git clone https://github.com/onnela-lab/beiwe-backend.git 2>> {log}'
        .format(home=REMOTE_HOME_DIR, log=LOG_FILE))
    
    # Make sure the code is on the right branch
    # git checkout prints to both stderr *and* stdout, so redirect them both to the log file
    run('cd {home}/beiwe-backend; git checkout master 1>> {log} 2>> {log}'
        .format(home=REMOTE_HOME_DIR, log=LOG_FILE))


def setup_python(using_pyenv=False):
    """
    Install pyenv as well as the latest version of Python 2, accessible
    via REMOTE_HOME_DIR/.pyenv/shims/python.
    """
    # using pyenv is deprecated, this section will be deleted.
    if using_pyenv:
        # Copy the installation script from the local repository onto the remote server,
        # make it executable and execute it.
        put(LOCAL_PYENV_INSTALLER_FILE, REMOTE_PYENV_INSTALLER_FILE)
        run('chmod +x {file}'.format(file=REMOTE_PYENV_INSTALLER_FILE))
        run(REMOTE_PYENV_INSTALLER_FILE)

        # Install the latest python 2 version and set pyenv to default to that version.
        # Note that this installation is slow, taking approximately a minute.
        # -f: Install even if the version appears to be installed already
        pyenv_exec = path_join(REMOTE_HOME_DIR, '.pyenv/bin/pyenv')
        run('{pyenv} install -f 2.7.14'.format(pyenv=pyenv_exec))
        run('{pyenv} global 2.7.14'.format(pyenv=pyenv_exec))

        # Display the version of python used by pyenv; this should print "Python 2.7.14".
        pyenv_shims_dir = path_join(REMOTE_HOME_DIR, '.pyenv/shims')
        run('{shims}/python --version'.format(shims=pyenv_shims_dir))

        # Upgrade pip, because we don't know what version the server came with.
        run('{shims}/pip install --upgrade pip >> {log}'.format(log=LOG_FILE, shims=pyenv_shims_dir))

        # Install the python requirements for running the server code. Note that we are using the
        # pyenv pip to ensure that the python requirements are installed into pyenv, rather than into
        #  the system python.
        run('{shims}/pip install -r {home}/beiwe-backend/Requirements.txt >> {log}'
            .format(home=REMOTE_HOME_DIR, log=LOG_FILE, shims=pyenv_shims_dir))
    else:
        sudo("pip install --upgrade pip")
        sudo('pip install -r {home}/beiwe-backend/Requirements.txt >> {log}'
            .format(home=REMOTE_HOME_DIR, log=LOG_FILE))


def setup_celery_worker():
    # Copy the script from the local repository onto the remote server,
    # make it executable and execute it.
    put(LOCAL_INSTALL_CELERY_WORKER, REMOTE_INSTALL_CELERY_WORKER)
    run('chmod +x {file}'.format(file=REMOTE_INSTALL_CELERY_WORKER))
    run('{file} >> {log}'.format(file=REMOTE_INSTALL_CELERY_WORKER, log=LOG_FILE))


def setup_worker_cron():
    # Copy the cronjob file onto the remote server and add it to the remote crontab
    put(LOCAL_CRONJOB_WORKER_FILE_PATH, REMOTE_CRONJOB_FILE_PATH)
    run('crontab -u {user} {file}'.format(file=REMOTE_CRONJOB_FILE_PATH, user=REMOTE_USERNAME))


def setup_manager_cron():
    # Copy the cronjob file onto the remote server and add it to the remote crontab
    put(LOCAL_CRONJOB_MANAGER_FILE_PATH, REMOTE_CRONJOB_FILE_PATH)
    run('crontab -u {user} {file}'.format(file=REMOTE_CRONJOB_FILE_PATH, user=REMOTE_USERNAME))


def setup_rabbitmq():
    # push the conviguration file so that it listens on the configured port
    put(LOCAL_RABBIT_MQ_CONFIG_FILE_PATH, REMOTE_RABBIT_MQ_CONFIG_FILE_PATH)
    sudo("cp {source} {dest}".format(
            source=REMOTE_RABBIT_MQ_CONFIG_FILE_PATH, dest=REMOTE_RABBIT_MQ_FINAL_CONFIG_FILE_PATH
    ))
    
    # setup a new password
    create_rabbit_mq_password()
    sudo("rabbitmqctl add_user beiwe {password}".format(password=get_rabbit_mq_password()))
    sudo('rabbitmqctl set_permissions -p / beiwe ".*" ".*" ".*"')
    sudo("service rabbitmq-server restart")
    

def setup_single_server_ami_cron():
    put(LOCAL_CRONJOB_SINGLE_SERVER_AMI_FILE_PATH, REMOTE_CRONJOB_FILE_PATH)
    run('crontab -u {user} {file}'.format(file=REMOTE_CRONJOB_FILE_PATH, user=REMOTE_USERNAME))


def apt_installs(manager=False, single_server_ami=False):
    
    if manager:
        apt_install_list = APT_MANAGER_INSTALLS
    elif single_server_ami:
        apt_install_list = APT_SINGLE_SERVER_AMI_INSTALLS
    else:
        apt_install_list = APT_WORKER_INSTALLS
    installs_string = " ".join(apt_install_list)
    
    # Sometimes (usually on slower servers) the remote server isn't done with initial setup when
    # we get to this step, so it has a bunch of retry logic.
    installs_failed = True
    warning_printed = False
    for i in range(10):
        try:
            sudo('apt-get -y update >> {log}'.format(log=LOG_FILE))
            sudo('apt-get -y install {installs} >> {log}'.format(installs=installs_string, log=LOG_FILE))
            installs_failed = False
            break
        except FabricExecutionError:
            if not warning_printed:
                log.warning(
                        "WARNING: encountered problems when trying to run apt installs.\n"
                        "Usually this means the server is running a software upgrade in the background.\n"
                        "Will try 10 times, waiting 5 seconds each time."
                )
            sleep(5)
    
    if installs_failed:
        raise Exception("Could not install software on remote machine.")

def push_beiwe_configuration(eb_environment_name, single_server_ami=False):
    # single server ami gets the demmy environment file, cluster gets the customized one.
    if single_server_ami:
        put(LOCAL_AMI_ENV_CONFIG_FILE_PATH,
            DEPLOYMENT_ENVIRON_SETTING_REMOTE_FILE_PATH)
    else:
        put(get_pushed_full_processing_server_env_file_path(eb_environment_name),
            DEPLOYMENT_ENVIRON_SETTING_REMOTE_FILE_PATH)


def configure_apache():
    put(LOCAL_APACHE_CONFIG_FILE_PATH, REMOTE_APACHE_CONFIG_FILE_PATH)
    sudo("mv {file} /etc/apache2/sites-available/000-default.conf".format(file=REMOTE_APACHE_CONFIG_FILE_PATH))
    sudo("service apache2 restart")


def configure_local_postgres():
    run("sudo -u postgres createuser -d -r -s ubuntu")
    run("sudo -u postgres createdb ubuntu")
    run('psql -U ubuntu -c "CREATE DATABASE beiweproject"')
    run('psql -U ubuntu -c "CREATE USER beiweuser WITH PASSWORD \'password\'"')
    run('psql -U ubuntu -c "GRANT ALL PRIVILEGES ON DATABASE beiweproject TO beiweuser"')

####################################################################################################
#################################### CLI Utility ###################################################
####################################################################################################

def do_fail_if_environment_does_not_exist(name):
    environment_exists = check_if_eb_environment_exists(name)
    if not environment_exists:
        log.error("There is already an environment named '%s'" % name.lower())
        EXIT(1)


def do_fail_if_environment_exists(name):
    environment_exists = check_if_eb_environment_exists(name)
    if environment_exists:
        log.error("There is already an environment named '%s'" % name.lower())
        EXIT(1)


def do_fail_if_bad_environment_name(name):
    if not (4 <= len(name) < 40):
        log.error("That name is either too long or too short.")
        EXIT(1)
    
    if not re.match("^[a-zA-Z0-9-]+$", name) or name.endswith("-"):
        log.error("that is not a valid Elastic Beanstalk environment name.")
        EXIT(1)


def prompt_for_new_eb_environment_name(with_prompt=True):
    if with_prompt:
        print(ENVIRONMENT_NAME_RESTRICTIONS)
    name = input()
    do_fail_if_bad_environment_name(name)
    return name


def prompt_for_extant_eb_environment_name():
    print(EXTANT_ENVIRONMENT_PROMPT)
    name = input()
    environment_exists = check_if_eb_environment_exists(name)
    if not environment_exists:
        log.error("There is no environment with the name %s" % environment_exists)
        EXIT(1)
    validate_beiwe_environment_config(name)
    return name

####################################################################################################
##################################### AWS Operations ###############################################
####################################################################################################

def do_setup_eb_update():
    print("\n", DO_SETUP_EB_UPDATE_OPEN)
    
    files = sorted([f for f in os.listdir(STAGED_FILES) if f.lower().endswith(".zip")])
    
    if not files:
        print("Could not find any zip files in " + STAGED_FILES)
        EXIT(1)
    
    print("Enter the version of the codebase do you want to use:")
    for i, file_name in enumerate(files):
        print("[%s]: %s" % (i + 1, file_name))
    print("(press CTL-C to cancel)\n")
    try:
        index = int(input("$ "))
    except Exception:
        log.error("Could not parse input.")
        EXIT(1)
    
    if index < 1 or index > len(files):
        log.error("%s was not a valid option." % index)
        EXIT(1)
    
    # handle 1-indexing
    file_name = files[index - 1]
    # log.info("Processing %s..." % file_name)
    time_ext = current_time_string().replace(" ", "_").replace(":", "_")
    output_file_name = file_name[:-4] + "_processed_" + time_ext + ".zip"
    do_zip_reduction(file_name, STAGED_FILES, output_file_name)
    log.info("Done processing %s." % file_name)
    log.info("The new file %s has been placed in %s" % (output_file_name, STAGED_FILES))
    print("You can now provide Elastic Beanstalk with %s to run an automated deployment of the new code." % output_file_name)
    EXIT(0)


def do_create_environment():
    print(DO_CREATE_ENVIRONMENT)
    name = prompt_for_new_eb_environment_name(with_prompt=False)
    do_fail_if_bad_environment_name(name)
    do_fail_if_environment_exists(name)
    validate_beiwe_environment_config(name)  # Exits if any non-autogenerated credentials are bad.
    create_new_rds_instance(name)
    create_finalized_configuration(name)
    create_eb_environment(name)
    log.info("Created Beiwe cluster environment successfully")


def do_help_setup_new_environment():
    print(HELP_SETUP_NEW_ENVIRONMENT)
    name = prompt_for_new_eb_environment_name()
    do_fail_if_bad_environment_name(name)
    do_fail_if_environment_exists(name)

    beiwe_environment_fp = get_beiwe_python_environment_variables_file_path(name)
    processing_server_settings_fp = get_server_configuration_file_path(name)
    extant_files = os.listdir(USER_SPECIFIC_CONFIG_FOLDER)
    
    for fp in (beiwe_environment_fp, processing_server_settings_fp):
        if os.path.basename(fp) in extant_files:
            log.error("is already a file at %s" % relpath(beiwe_environment_fp))
            EXIT(1)
    
    with open(beiwe_environment_fp, 'w') as f:
        json.dump(reference_environment_configuration_file(), f, indent=1)
    with open(processing_server_settings_fp, 'w') as f:
        json.dump(reference_data_processing_server_configuration(), f, indent=1)
    
    print("Environment specific files have been created at %s and %s." % (
        relpath(beiwe_environment_fp),
        relpath(processing_server_settings_fp),
    ))
    
    # Note: we actually cannot generate RDS credentials until we have a server, this is because
    # the hostname cannot exist until the server exists.
    print("""After filling in the required contents of these newly created files you will be able
    to run the -create-environment command.  Note that several more credentials files will be
    generated as part of that process. """)
    

def do_create_manager():
    name = prompt_for_extant_eb_environment_name()
    do_fail_if_environment_does_not_exist(name)
    create_processing_server_configuration_file(name)
    
    try:
        settings = get_server_configuration_file(name)
    except Exception as e:
        log.error("could not read settings file")
        log.error(e)
        EXIT(1)
        
    log.info("creating manager server for %s..." % name)
    try:
        instance = create_processing_control_server(name, settings["MANAGER_SERVER_INSTANCE_TYPE"])
    except Exception as e:
        log.error(e)
        EXIT(1)
    public_ip = instance['NetworkInterfaces'][0]['PrivateIpAddresses'][0]['Association']['PublicIp']
    
    configure_fabric(name, public_ip)
    push_files()
    apt_installs(manager=True)
    setup_rabbitmq()
    load_git_repo()
    setup_python()
    push_beiwe_configuration(name)
    push_manager_private_ip_and_password(name)
    setup_manager_cron()


def do_create_worker():
    name = prompt_for_extant_eb_environment_name()
    do_fail_if_environment_does_not_exist(name)
    manager_instance = get_manager_instance_by_eb_environment_name(name)
    if manager_instance is None:
        log.error(
            "There is no manager server for the %s cluster, cannot deploy a worker until there is." % name)
        EXIT(1)
    
    manager_public_ip = get_manager_public_ip(name)
    manager_private_ip = get_manager_private_ip(name)
    
    try:
        settings = get_server_configuration_file(name)
    except Exception as e:
        log.error("could not read settings file")
        log.error(e)
        EXIT(1)
    
    log.info("creating worker server for %s..." % name)
    try:
        instance = create_processing_server(name, settings["MANAGER_SERVER_INSTANCE_TYPE"])
    except Exception as e:
        log.error(e)
        EXIT(1)
    instance_ip = instance['NetworkInterfaces'][0]['PrivateIpAddresses'][0]['Association']['PublicIp']
    # TODO: fabric up the worker with the celery/supervisord and ensure it can connect to manager.
    
    configure_fabric(name, instance_ip)
    push_files()
    apt_installs()
    load_git_repo()
    setup_python()
    push_beiwe_configuration(name)
    push_manager_private_ip_and_password(name)
    setup_celery_worker()
    setup_worker_cron()


def do_create_single_server_ami(ip_address, key_filename):
    """
    Set up a beiwe-backend deployment on a single server suitable for turning into an AMI
    :param ip_address: IP address of the server you're setting up as an AMI
    :param key_filename: Full filepath of the key that lets you SSH into that server
    """
    configure_fabric(None, ip_address, key_filename=key_filename)
    push_files()
    apt_installs(single_server_ami=True)
    load_git_repo()
    """ Don't install .pyenv, because the single-server AMI uses mod_wsgi, which doesn't work
    with .pyenv.  Just use the system version of Python. As of Dec 3, 2017, the system version
    of Python is 2.7.12, which is good enough to run Beiwe). """
    setup_python(using_pyenv=False)
    push_beiwe_configuration(None, single_server_ami=True)
    configure_local_postgres()
    manage_script_filepath = path_join(REMOTE_HOME_DIR, "beiwe-backend/manage.py")
    run('python {filename} migrate'.format(filename=manage_script_filepath))
    run('python {filename} create_default_login'.format(filename=manage_script_filepath))
    setup_single_server_ami_cron()
    configure_apache()
    remove_unneeded_ssh_keys()
    
    
def do_fix_health_checks():
    name = prompt_for_extant_eb_environment_name()
    do_fail_if_environment_does_not_exist(name)
    try:
        print("Setting environment to ignore health checks")
        fix_deploy(name)
    except Exception as e:
        log.error("unable to run command due to the following error:\n %s" % e)
        raise
    print("Success.")
    

####################################################################################################
####################################### Validation #################################################
####################################################################################################
purge_command_blurb = """
DO NOT RUN THIS COMMAND ON A FUNCTIONAL ELASTIC BEANSTALK DEPLOYMENT.
Only run this if you are having first-run deployment issues and only if you want to start over.

This command exists because Instance Profiles are not fully-exposed on the AWS Console website and you will not be able to appropriately clear out all of the IAM entities for the

Note 1: Run this command repeatedly until it tells you it cannot delete anything.
Note 2: You may have to go and manually delete a Service Role if you are intent on totally resetting your Elastic Beanstalk cluster.
"""

def cli_args_validation():
    # Warning: any change to format here requires you re-check all parameter validation
    parser = argparse.ArgumentParser(
             description="interactive set of commands for deploying a Beiwe Cluster")
    # Use '"count"' as the type, don't try and be fancy, argparse is a pain.
    parser.add_argument(
            '-create-environment',
            action="count",
            help="creates new environment with the provided environment name"
    )
    parser.add_argument(
            '-create-manager',
            action="count",
            help="creates a data processing manager for the provided environment"
    )
    parser.add_argument(
            '-create-worker',
            action="count",
            help="creates a data processing worker for the provided environment"
    )
    parser.add_argument(
            "-help-setup-new-environment",
            action="count",
            help= "assists in creation of configuration files for a beiwe environment deployment"
    )
    parser.add_argument(
            "-fix-health-checks-blocking-deployment",
            action="count",
            help="sometimes deployment operations fail stating that health checks do not have sufficient permissions, run this command to fix that."
    )
    parser.add_argument(
            "-purge-instance-profiles",
            action="count",
            help=purge_command_blurb
    )
    
    
    # Note: this arguments variable is not iterable.
    # access entities as arguments.long_name_of_argument, like arguments.update_manager
    arguments = parser.parse_args()

    # print help message if no arguments were supplied
    if len(sys.argv) == 1:
        parser.print_help()
        EXIT()

    return arguments


####################################################################################################
##################################### Argument Parsing #############################################
####################################################################################################

if __name__ == "__main__":
    # validate the global configuration file
    if not all((are_aws_credentials_present(), is_global_configuration_valid())):
        EXIT(1)
    
    # get CLI arguments, see function for details
    arguments = cli_args_validation()
    # pprint (vars(arguments))
    
    if arguments.help_setup_new_environment:
        do_help_setup_new_environment()
        EXIT(0)

    if arguments.create_environment:
        do_create_environment()
        EXIT(0)
        
    if arguments.create_manager:
        do_create_manager()
        EXIT(0)
    
    if arguments.create_worker:
        do_create_worker()
        EXIT(0)

    if arguments.fix_health_checks_blocking_deployment:
        do_fix_health_checks()
        EXIT(0)
    
    if arguments.purge_instance_profiles:
        print(purge_command_blurb, "\n\n\n")
        iam_purge_instance_profiles()
        EXIT(0)
