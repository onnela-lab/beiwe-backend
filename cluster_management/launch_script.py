# TODO:
# determine how to integrate with existing aws creds
# import settings in an intelligent manner from some json config files
# code that validates and prints all parameters in the json files for review?

# uh-oh - SSL settings - I guess we need to validate route53 DNS settings.

# update data processing?
# stop data processing?
# deploy N data processing servers
import argparse
import json
import os
import re
import sys
from os import environ
from os.path import join as path_join, relpath
from time import sleep

from fabric.api import env as fabric_env, put, run, sudo

from deployment_helpers.aws.elastic_beanstalk import (check_if_eb_environment_exists,
    create_eb_environment, fix_deploy)
from deployment_helpers.aws.elastic_compute_cloud import (create_processing_control_server,
    create_processing_server, get_manager_instance_by_eb_environment_name, get_manager_private_ip,
    get_manager_public_ip, get_worker_public_ips, terminate_all_processing_servers)
from deployment_helpers.aws.iam import iam_purge_instance_profiles
from deployment_helpers.aws.rds import create_new_rds_instance
from deployment_helpers.configuration_utils import (are_aws_credentials_present,
    create_finalized_configuration, create_processing_server_configuration_file,
    create_rabbit_mq_password_file, get_rabbit_mq_password,
    is_global_configuration_valid, reference_data_processing_server_configuration,
    reference_environment_configuration_file, validate_beiwe_environment_config)
from deployment_helpers.constants import (APT_MANAGER_INSTALLS, APT_SINGLE_SERVER_AMI_INSTALLS,
    APT_WORKER_INSTALLS, CREATE_ENVIRONMENT_HELP, CREATE_MANAGER_HELP, CREATE_WORKER_HELP,
    DEPLOYMENT_ENVIRON_SETTING_REMOTE_FILE_PATH, DEPLOYMENT_SPECIFIC_CONFIG_FOLDER, DEV_HELP,
    DEV_MODE, DO_CREATE_ENVIRONMENT, DO_SETUP_EB_UPDATE_OPEN, ENVIRONMENT_NAME_RESTRICTIONS,
    EXTANT_ENVIRONMENT_PROMPT, FILES_TO_PUSH, FIX_HEALTH_CHECKS_BLOCKING_DEPLOYMENT_HELP,
    get_beiwe_python_environment_variables_file_path, get_finalized_environment_variables,
    get_global_config, GET_MANAGER_IP_ADDRESS_HELP, get_pushed_full_processing_server_env_file_path,
    get_server_configuration_file, get_server_configuration_file_path, GET_WORKER_IP_ADDRESS_HELP,
    HELP_SETUP_NEW_ENVIRONMENT, HELP_SETUP_NEW_ENVIRONMENT_HELP, LOCAL_AMI_ENV_CONFIG_FILE_PATH,
    LOCAL_APACHE_CONFIG_FILE_PATH, LOCAL_CRONJOB_MANAGER_FILE_PATH,
    LOCAL_CRONJOB_SINGLE_SERVER_AMI_FILE_PATH, LOCAL_CRONJOB_WORKER_FILE_PATH,
    LOCAL_INSTALL_CELERY_WORKER, LOCAL_RABBIT_MQ_CONFIG_FILE_PATH, LOG_FILE,
    MANAGER_SERVER_INSTANCE_TYPE, PROD_HELP, PROD_MODE, PURGE_COMMAND_BLURB,
    PURGE_INSTANCE_PROFILES_HELP, PUSHED_FILES_FOLDER, RABBIT_MQ_PORT,
    REMOTE_APACHE_CONFIG_FILE_PATH, REMOTE_CRONJOB_FILE_PATH, REMOTE_HOME_DIR,
    REMOTE_INSTALL_CELERY_WORKER, REMOTE_RABBIT_MQ_CONFIG_FILE_PATH,
    REMOTE_RABBIT_MQ_FINAL_CONFIG_FILE_PATH, REMOTE_RABBIT_MQ_PASSWORD_FILE_PATH, REMOTE_USERNAME,
    STAGED_FILES, TERMINATE_PROCESSING_SERVERS_HELP, WORKER_SERVER_INSTANCE_TYPE)
from deployment_helpers.general_utils import current_time_string, do_zip_reduction, EXIT, log, retry


# Fabric configuration
class FabricExecutionError(Exception): pass
fabric_env.abort_exception = FabricExecutionError
fabric_env.abort_on_prompts = True

parser = argparse.ArgumentParser(description="interactive set of commands for deploying a Beiwe Cluster")

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


def try_run(*args, **kwargs):
    try:
        run(*args, **kwargs)
    except FabricExecutionError:
        pass

def try_sudo(*args, **kwargs):
    try:
        sudo(*args, **kwargs)
    except FabricExecutionError:
        pass


####################################################################################################
##################################### Server Config ################################################
####################################################################################################


def remove_unneeded_ssh_keys():
    """ This is based on a recommendation from AWS documentation:
    https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/
    building-shared-amis.html?icmpid=docs_ec2_console#remove-ssh-host-key-pairs
    Once you run this, you can't SSH into the server until you've rebooted it. """
    sudo("shred -u /etc/ssh/*_key /etc/ssh/*_key.pub")


def push_manager_private_ip_and_password(eb_environment_name):
    ip = get_manager_private_ip(eb_environment_name) + ":" + str(RABBIT_MQ_PORT)
    password = get_rabbit_mq_password(eb_environment_name)

    # echo puts a new line at the end of the output
    run(f"echo {ip} > {REMOTE_RABBIT_MQ_PASSWORD_FILE_PATH}")
    run(f"printf {password} >> {REMOTE_RABBIT_MQ_PASSWORD_FILE_PATH}")


def push_home_directory_files():
    for local_relative_file, remote_relative_file in FILES_TO_PUSH:
        local_file_path = path_join(PUSHED_FILES_FOLDER, local_relative_file)
        remote_file_path = path_join(REMOTE_HOME_DIR, remote_relative_file)
        put(local_file_path, remote_file_path)


def load_git_repo():
    """ Get a local copy of the git repository """
    # Git clone the repository into the remote beiwe-backend folder
    # git operations print to both stderr *and* stdout, so redirect them both to the log file
    run(f'cd {REMOTE_HOME_DIR}; git clone https://github.com/onnela-lab/beiwe-backend.git 2>> {LOG_FILE}')

    if DEV_MODE:
        branch = environ.get("DEV_BRANCH", "development")
    elif PROD_MODE:
        branch = "production"
    else:
        branch = "master"

    run(f'cd {REMOTE_HOME_DIR}/beiwe-backend; git checkout {branch} 1>> {LOG_FILE} 2>> {LOG_FILE}')


def setup_python():
    """ Installs requirements. """
    # sudo required because we are using the
    sudo("python3 -m pip install --upgrade pip==21.0.1 setuptools==53.0.0 >> {log}")
    run('python3 -m pip install --user -r {home}/beiwe-backend/requirements.txt >> {log}'.
         format(home=REMOTE_HOME_DIR, log=LOG_FILE))
    run('python3 -m pip install --user -r {home}/beiwe-backend/requirements_data_processing.txt >> {log}'.
         format(home=REMOTE_HOME_DIR, log=LOG_FILE))


def setup_celery_worker():
    # Copy the script from the local repository onto the remote server,
    # make it executable and execute it.
    put(LOCAL_INSTALL_CELERY_WORKER, REMOTE_INSTALL_CELERY_WORKER)
    run('chmod +x {file}'.format(file=REMOTE_INSTALL_CELERY_WORKER))
    run('{file} >> {log}'.format(file=REMOTE_INSTALL_CELERY_WORKER, log=LOG_FILE))


def manager_fix():
    # It is unclear what causes this.  The notifications task create zombie processes that on at
    # least one occasion did not respond to kill -9 commands even when run as the superuser. This
    # occurs on both workers and managers, a 20 second sleep operation fixes it, 10 seconds does not.
    # Tested on the slowest server, t3a.nano' with swap that is required to run the celery tasks.)

    # Update: it turns out there is an alternate failure mode if you try to do the 20 second
    # wait (which works for workers), which is that all calls to the celery Inspect object
    # block for exceptionally long periods, even when a timeout value is provided. (This behavior
    # has other triggers too, this is just a reliable way to trigger it.)
    try_sudo("shutdown -r now")
    log.warning("rebooting server to fix rabbitmq bugs...")
    sleep(5)
    retry(run, "# waiting for server to reboot, this might take a while.")

    # we need to re-enable the swap after the reboot, then we can finally start supervisor without
    # creating zombie celery threads.
    sudo("swapon /swapfile")
    sudo("swapon -s")


def setup_worker_cron():
    # Copy the cronjob file onto the remote server and add it to the remote crontab
    put(LOCAL_CRONJOB_WORKER_FILE_PATH, REMOTE_CRONJOB_FILE_PATH)
    run('crontab -u {user} {file}'.format(file=REMOTE_CRONJOB_FILE_PATH, user=REMOTE_USERNAME))


def setup_manager_cron():
    # Copy the cronjob file onto the remote server and add it to the remote crontab
    put(LOCAL_CRONJOB_MANAGER_FILE_PATH, REMOTE_CRONJOB_FILE_PATH)
    run('crontab -u {user} {file}'.format(file=REMOTE_CRONJOB_FILE_PATH, user=REMOTE_USERNAME))


def setup_rabbitmq(eb_environment_name):
    create_rabbit_mq_password_file(eb_environment_name)

    # push the configuration file so that it listens on the configured port
    put(LOCAL_RABBIT_MQ_CONFIG_FILE_PATH, REMOTE_RABBIT_MQ_CONFIG_FILE_PATH)
    sudo(f"cp {REMOTE_RABBIT_MQ_CONFIG_FILE_PATH} {REMOTE_RABBIT_MQ_FINAL_CONFIG_FILE_PATH}")

    # setup a new password
    sudo(f"rabbitmqctl add_user beiwe {get_rabbit_mq_password(eb_environment_name)}")
    sudo('rabbitmqctl set_permissions -p / beiwe ".*" ".*" ".*"')


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
    for i in range(10):
        try:
            sudo('apt-get -y update >> {log}'.format(log=LOG_FILE))
            sudo('apt-get -y install {installs} >> {log}'.format(installs=installs_string, log=LOG_FILE))
            installs_failed = False
            break
        except FabricExecutionError:
            log.warning(
                "WARNING: encountered problems when trying to run apt installs.\n"
                "Usually this means the server is running a software upgrade in the background.\n"
                "Will try 10 times, waiting 5 seconds each time."
            )
            sleep(5)

    # we run supervisor manually at the end
    sudo("service supervisor stop")
    if installs_failed:
        raise Exception("Could not install software on remote machine.")


def setup_single_server_ami_cron():
    put(LOCAL_CRONJOB_SINGLE_SERVER_AMI_FILE_PATH, REMOTE_CRONJOB_FILE_PATH)
    run('crontab -u {user} {file}'.format(file=REMOTE_CRONJOB_FILE_PATH, user=REMOTE_USERNAME))


def push_beiwe_configuration(eb_environment_name, single_server_ami=False):
    # single server ami gets the dummy environment file, cluster gets the customized one.
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


def create_swap():
    """
    Allows the use of tiny T series servers and in general is nice to have.
    fallocate -l 4G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile && swapon -s
    """
    sudo("fallocate -l 4G /swapfile")
    sudo("chmod 600 /swapfile")
    sudo("mkswap /swapfile")
    sudo("swapon /swapfile")
    sudo("swapon -s")


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
        log.error("There is no environment with the name %s" % name)
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
        index = None  # ide warnings
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
    extant_files = os.listdir(DEPLOYMENT_SPECIFIC_CONFIG_FOLDER)

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
        settings = None  # ide warnings...
        EXIT(1)

    log.info("creating manager server for %s..." % name)
    try:
        instance = create_processing_control_server(name, settings[MANAGER_SERVER_INSTANCE_TYPE])
    except Exception as e:
        log.error(e)
        instance = None  # ide warnings...
        EXIT(1)
    public_ip = instance['NetworkInterfaces'][0]['PrivateIpAddresses'][0]['Association']['PublicIp']

    configure_fabric(name, public_ip)
    create_swap()
    push_home_directory_files()
    apt_installs(manager=True)
    setup_rabbitmq(name)
    load_git_repo()
    setup_python()
    push_beiwe_configuration(name)
    push_manager_private_ip_and_password(name)
    setup_manager_cron()
    setup_celery_worker()  # run setup worker last.
    manager_fix()
    run("supervisord")


def do_create_worker():
    name = prompt_for_extant_eb_environment_name()
    do_fail_if_environment_does_not_exist(name)
    manager_instance = get_manager_instance_by_eb_environment_name(name)
    if manager_instance is None:
        log.error(
            "There is no manager server for the %s cluster, cannot deploy a worker until there is." % name)
        EXIT(1)

    try:
        settings = get_server_configuration_file(name)
    except Exception as e:
        log.error("could not read settings file")
        log.error(e)
        settings = None  # ide warnings...
        EXIT(1)

    log.info("creating worker server for %s..." % name)
    try:
        instance = create_processing_server(name, settings[WORKER_SERVER_INSTANCE_TYPE])
    except Exception as e:
        log.error(e)
        instance = None  # ide warnings...
        EXIT(1)
    instance_ip = instance['NetworkInterfaces'][0]['PrivateIpAddresses'][0]['Association']['PublicIp']

    configure_fabric(name, instance_ip)
    create_swap()
    push_home_directory_files()
    apt_installs()
    load_git_repo()
    setup_python()
    push_beiwe_configuration(name)
    push_manager_private_ip_and_password(name)
    setup_worker_cron()
    setup_celery_worker()  # run setup worker last.
    log.warning("Server is almost up.  Waiting 20 seconds to avoid a race condition...")
    sleep(20)
    run("supervisord")


def do_create_single_server_ami(ip_address, key_filename):
    """
    Set up a beiwe-backend deployment on a single server suitable for turning into an AMI
    :param ip_address: IP address of the server you're setting up as an AMI
    :param key_filename: Full filepath of the key that lets you SSH into that server
    """
    configure_fabric(None, ip_address, key_filename=key_filename)
    push_home_directory_files()
    apt_installs(single_server_ami=True)
    load_git_repo()
    setup_python()
    push_beiwe_configuration(None, single_server_ami=True)
    configure_local_postgres()
    manage_script_filepath = path_join(REMOTE_HOME_DIR, "beiwe-backend/manage.py")
    run('python3 {filename} migrate'.format(filename=manage_script_filepath))
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


def do_terminate_all_processing_servers():
    name = prompt_for_extant_eb_environment_name()
    do_fail_if_environment_does_not_exist(name)
    terminate_all_processing_servers(name)


def do_get_manager_ip_address():
    name = prompt_for_extant_eb_environment_name()
    do_fail_if_environment_does_not_exist(name)
    log.info(f"The IP address of the manager server for {name} is {get_manager_public_ip(name)}")


def do_get_worker_ip_addresses():
    name = prompt_for_extant_eb_environment_name()
    do_fail_if_environment_does_not_exist(name)
    ips = ', '.join(get_worker_public_ips(name))
    if ips:
        log.info(f"The IP address of the worker servers for {name} are {ips}")


####################################################################################################
####################################### Validation #################################################
####################################################################################################


def cli_args_validation():
    # Use '"count"' as the type, don't try and be fancy, argparse is a pain.
    parser.add_argument('-create-environment', action="count", help=CREATE_ENVIRONMENT_HELP)
    parser.add_argument('-create-manager', action="count", help=CREATE_MANAGER_HELP)
    parser.add_argument('-create-worker', action="count", help=CREATE_WORKER_HELP)
    parser.add_argument("-help-setup-new-environment", action="count", help=HELP_SETUP_NEW_ENVIRONMENT_HELP)
    parser.add_argument("-fix-health-checks-blocking-deployment", action="count", help=FIX_HEALTH_CHECKS_BLOCKING_DEPLOYMENT_HELP)
    parser.add_argument("-dev", action="count", help=DEV_HELP)
    parser.add_argument("-prod", action="count", help=PROD_HELP)
    parser.add_argument("-purge-instance-profiles", action="count", help=PURGE_INSTANCE_PROFILES_HELP)
    parser.add_argument("-terminate-processing-servers", action="count", help=TERMINATE_PROCESSING_SERVERS_HELP)
    parser.add_argument('-get-manager-ip', action="count", help=GET_MANAGER_IP_ADDRESS_HELP)
    parser.add_argument('-get-worker-ips', action="count", help=GET_WORKER_IP_ADDRESS_HELP)

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

    if arguments.prod:
        log.warning("RUNNING IN PROD MODE")
        PROD_MODE.set(True)

    if arguments.dev:
        if PROD_MODE:
            log.error("You cannot provide -prod and -dev at the same time.")
            EXIT(1)
        DEV_MODE.set(True)
        log.warning("RUNNING IN DEV MODE")

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
        print(PURGE_COMMAND_BLURB, "\n\n\n")
        iam_purge_instance_profiles()
        EXIT(0)

    if arguments.terminate_processing_servers:
        do_terminate_all_processing_servers()
        EXIT(0)

    if arguments.get_manager_ip:
        do_get_manager_ip_address()
        EXIT(0)

    if arguments.get_worker_ips:
        do_get_worker_ip_addresses()
        EXIT(0)

    # print help if nothing else did (make just supplying -dev print the help screen)
    parser.print_help()
    EXIT(0)
