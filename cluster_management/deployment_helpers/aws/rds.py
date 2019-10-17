import json
import os
from time import sleep

from deployment_helpers.aws.boto_helpers import create_ec2_resource, create_rds_client
from deployment_helpers.aws.security_groups import (
    create_sec_grp_rule_parameters_allowing_traffic_from_another_security_group,
    create_security_group, get_security_group_by_name)
from deployment_helpers.constants import (DBInstanceNotFound, get_db_credentials_file_path,
    get_server_configuration_file)
from deployment_helpers.general_utils import (EXIT, current_time_string, log,
    random_alphanumeric_starting_with_letter, random_alphanumeric_string)

# t1.micro, m1.small, m1.medium, m1.large, m1.xlarge, m2.xlarge, m2.2xlarge, m2.4xlarge,
# m3.medium, m3.large, m3.xlarge, m3.2xlarge, m4.large, m4.xlarge, m4.2xlarge, m4.4xlarge,
# m4.10xlarge, r3.large, r3.xlarge, r3.2xlarge, r3.4xlarge, r3.8xlarge, t2.micro, t2.small,
# t2.medium, and t2.large.
database_sec_group_description = \
    "provides postgres access to everything with security group %s"

instance_database_sec_group_description = \
    "provides postgres access to the postgres database %s"

MAINTAINANCE_WINDOW = "sat:08:00-sat:09:00"  # utc
BACKUP_WINDOW = "04:34-05:04"
BACKUP_RETENTION_PERIOD_DAYS = 35  # 35 is the maximum backup period from the AWS console.
POSTGRES_PORT = 5432  # this is the default postgres port.

# aws documentation for storage: http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_Storage.html
# boto docs: http://boto3.readthedocs.io/en/latest/reference/services/rds.html
# TODO: database encryption?


####################################################################################################
#################################### Database Credentials ##########################################
####################################################################################################

def generate_valid_postgres_credentials():
    """ Generates and prints credentials for database access. """
    credentials = {
        # 1 to 63 alphanumeric characters, first character must be a letter.
        "RDS_USERNAME": random_alphanumeric_starting_with_letter(63),
        # 8 to 128 characters, typeable characters, not /, ", or @
        "RDS_PASSWORD": random_alphanumeric_string(128),
        # 1 to 63 alphanumeric characters, begin with a letter
        "RDS_DB_NAME": random_alphanumeric_starting_with_letter(63)
    }
    print("database username:", credentials['RDS_USERNAME'])
    print("database password:", credentials['RDS_PASSWORD'])
    print("database name:", credentials['RDS_DB_NAME'])
    return credentials


def get_full_db_credentials(eb_environment_name):
    """ reads in the database credentials for the project and dynamically identifies the hostname
    parameter. """
    with open(get_db_credentials_file_path(eb_environment_name), 'r') as f:
        credentials = json.load(f)
    credentials['RDS_HOSTNAME'] = get_db_info(eb_environment_name)['Endpoint']['Address']
    return credentials


def write_rds_credentials(eb_environment_name, credentials, test_for_existing_files):
    """ Writes to the database credentials file for the environment. """
    db_credentials_path = get_db_credentials_file_path(eb_environment_name)
    if test_for_existing_files and os.path.exists(db_credentials_path):
        msg = "Encountered a file at %s, abortiing." % db_credentials_path
        log.error("Encountered a file at %s, abortiing.")
        raise Exception(msg)

    with open(db_credentials_path, 'w') as f:
        json.dump(credentials, f, indent=1)
        log.info("database credentials have been written to %s" % db_credentials_path)

####################################################################################################
##################################### RDS Security Groups ##########################################
####################################################################################################

def construct_db_name(eb_environment_name):
    return eb_environment_name + '-database'


def get_rds_security_group_names(db_identifier):
    instance_sec_group_name = db_identifier + "_database_access"
    database_sec_group_name = db_identifier + "_allow_database_connections"
    return instance_sec_group_name, database_sec_group_name


def get_rds_security_groups_by_eb_name(eb_environment_name):
    return get_rds_security_groups(construct_db_name(eb_environment_name))


def get_rds_security_groups(db_identifier):
    instance_sec_grp_name, database_sec_grp_name = get_rds_security_group_names(db_identifier)
    return {
        "instance_sec_grp": get_security_group_by_name(instance_sec_grp_name),
        "database_sec_grp": get_security_group_by_name(database_sec_grp_name),
    }


def create_rds_security_groups(db_identifier):
    instance_sec_grp_name, database_sec_grp_name = get_rds_security_group_names(db_identifier)

    # create the security group that we will then allow for the database
    instance_sec_grp = create_security_group(instance_sec_grp_name,
                                             instance_database_sec_group_description % db_identifier)

    # construct the rule that will allow access from anything with the above security group
    ingress_rule = create_sec_grp_rule_parameters_allowing_traffic_from_another_security_group(
            POSTGRES_PORT,
            instance_sec_grp["GroupId"]
    )
    # and create the new security group
    create_security_group(database_sec_grp_name,
                          database_sec_group_description % instance_sec_grp_name,
                          list_of_dicts_of_ingress_kwargs=[ingress_rule])


def add_eb_environment_to_rds_database_security_group(eb_environment_name, eb_sec_grp_id):
    """ We need to add the elastic beansalk environment to the security group that we assign to the RDS insnant"""
    ingress_params = create_sec_grp_rule_parameters_allowing_traffic_from_another_security_group(
            tcp_port=POSTGRES_PORT, sec_grp_id=eb_sec_grp_id
    )

    _, database_sec_grp_name = get_rds_security_group_names(construct_db_name(eb_environment_name))
    db_sec_grp = get_security_group_by_name(database_sec_grp_name)
    sec_grp_resource = create_ec2_resource().SecurityGroup(db_sec_grp['GroupId'])
    sec_grp_resource.authorize_ingress(**ingress_params)


####################################################################################################
##################################### RDS Configuration ############################################
####################################################################################################

def get_most_recent_postgres_engine():
    """ The database version query will be paginated (at time of writing providing the Engine
    parameter lists 30 items in the region) if the environment contains more than 100 database
    versions available.  Annoyingly, the most recent version of postgres is the last item in the
    paginated returns.
    (The pagination code was tested by not filtering by engine type and worked when committed.) """
    rds_client = create_rds_client()
    query_response = rds_client.describe_db_engine_versions(Engine="postgres")

    list_of_database_versions = []
    list_of_database_versions.extend(query_response['DBEngineVersions'])

    while 'Marker' in query_response:
        query_response = rds_client.describe_db_engine_versions(
                Marker=query_response['Marker'], Engine="postgres"
        )
        list_of_database_versions.extend(query_response['DBEngineVersions'])

    if not list_of_database_versions:
        raise Exception("Could not find most recent postgres version.")

    return list_of_database_versions[-1]



def get_db_info(eb_environment_name):
    db_identifier = construct_db_name(eb_environment_name)
    rds_client = create_rds_client()
    # documentaation says this should only return the one result when using DBInstanceIdentifier.
    try:
        return rds_client.describe_db_instances(DBInstanceIdentifier=db_identifier)['DBInstances'][0]
    except Exception as e:
        # boto3 throws unimportable errors.
        if e.__class__.__name__ == "DBInstanceNotFoundFault":
            raise DBInstanceNotFound(db_identifier)
        raise


def create_new_rds_instance(eb_environment_name):
    db_instance_identifier = construct_db_name(eb_environment_name)
    # identify whether there is already a database with this name, we don't want to
    try:
        _ = get_db_info(eb_environment_name)
        log.error("There is already a database named %s" % eb_environment_name)
        EXIT()
    except DBInstanceNotFound:
        pass

    database_server_type = get_server_configuration_file(eb_environment_name)['DB_SERVER_TYPE']
    engine = get_most_recent_postgres_engine()

    credentials = generate_valid_postgres_credentials()
    log.info("writing database credentials to disk, database address will be added later.")

    write_rds_credentials(eb_environment_name, credentials, True)

    # There is some weirdness involving security groups.  It looks like there is this concept of
    # non-vpc security groups, I am fairly certain that this interacts with cross-vpc, IAM based
    # database access.
    create_rds_security_groups(db_instance_identifier)
    db_sec_grp_id = get_rds_security_groups(db_instance_identifier)['database_sec_grp']['GroupId']

    log.info("Creating RDS Postgres database named %s" % db_instance_identifier)

    rds_client = create_rds_client()
    rds_instance = rds_client.create_db_instance(
            # server details
            DBInstanceIdentifier=db_instance_identifier,
            DBInstanceClass="db." + database_server_type,
            MultiAZ=False,

            PubliclyAccessible=False,
            Port=POSTGRES_PORT,

            # attach the security group that will allow access
            VpcSecurityGroupIds=[db_sec_grp_id],
            #TODO: is this even relevant?
            # providing the subnet is critical, not providing this value causes the db to be non-vpc
            # DBSubnetGroupName='string',

            # db storage
            StorageType='gp2',  # valid options are standard, gp2, io1
            # Iops=1000,  # multiple between 3 and 10 times the storage; only for use with io1.

            # AllocatedStorage has weird constraints:
            # General Purpose (SSD) storage (gp2): Must be an integer from 5 to 6144.
            # Provisioned IOPS storage (io1): Must be an integer from 100 to 6144.
            # Magnetic storage (standard): Must be an integer from 5 to 3072.
            AllocatedStorage=50,  # in gigabytes

            StorageEncrypted=True,  # Encryption of data at rest, drive encryption.
            # KmsKeyId='string',
            # TdeCredentialArn='string',  # probably not something we will implement
            # TdeCredentialPassword='string',  # probably not something we will implement

            # Security
            MasterUsername=credentials['RDS_USERNAME'],
            MasterUserPassword=credentials['RDS_PASSWORD'],
            DBName=credentials['RDS_DB_NAME'],

            EnableIAMDatabaseAuthentication=False,

            Engine=engine['Engine'],  # will be "postgres"
            EngineVersion=engine['EngineVersion'],  # most recent postgres version in this region.
            PreferredMaintenanceWindow=MAINTAINANCE_WINDOW,
            PreferredBackupWindow=BACKUP_WINDOW,
            AutoMinorVersionUpgrade=True,  # auto-upgrades are fantastic
            BackupRetentionPeriod=BACKUP_RETENTION_PERIOD_DAYS,

            Tags=[{'Key': 'BEIWE-NAME',
                   'Value': 'Beiwe postgres database for %s' % eb_environment_name},],

            # Enhanced monitoring, leave disabled
            # MonitoringInterval=5,  # in seconds, Valid Values: 0, 1, 5, 10, 15, 30, 60
            # MonitoringRoleArn='string',  # required for monitoring interval other than 0

            # near as I can tell this is the "insert postgres paratmeters here" section.
            # DBParameterGroupName='string',

            # AvailabilityZone='string',  # leave as default (random)
            # DBSecurityGroups=['strings'], # non-vpc rds instance settings
            # LicenseModel='string',
            # CharacterSetName='string',
            # OptionGroupName='string',  # don't think this is required.
            # Domain='string',  # has the phrase "active directory" in the description
            # DomainIAMRoleName='string',
            # CopyTagsToSnapshot=True | False,
            # Timezone='string',  # only used by MSSQL
            # DBClusterIdentifier='string',  #
            # EnablePerformanceInsights=True,  # Aurora specific
            # PerformanceInsightsKMSKeyId='string'  # Aurora specific
            # PromotionTier = 123,  # Aurora specific
    )

    while True:
        try:
            db = get_db_info(eb_environment_name)
        except DBInstanceNotFound:
            log.error("couldn't find database %s, hopefully this is a momentary glitch. Retrying.")
            sleep(5)
            continue
        log.info('%s: RDS instance status is %s, waiting until status is "Ready"'
                 % (current_time_string(), db['DBInstanceStatus']))
        # RDS spinup goes creating > backing up > available.
        if db['DBInstanceStatus'] in ["creating", 'backing-up']:
            sleep(5)
        elif db['DBInstanceStatus'] == "available":
            log.info("Database status is no longer 'creating', it is '%s'" % db['DBInstanceStatus'])
            break
        else:
            raise Exception('encountered unknown database state "%s"' % db['DBInstanceStatus'])

    return db
