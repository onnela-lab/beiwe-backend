from time import sleep

from botocore.exceptions import ClientError

from deployment_helpers.aws.boto_helpers import create_ec2_client, create_ec2_resource

from deployment_helpers.aws.rds import (get_rds_security_groups_by_eb_name)
from deployment_helpers.aws.security_groups import (
    create_sec_grp_rule_parameters_allowing_traffic_from_another_security_group,
    create_security_group, get_security_group_by_name, InvalidSecurityGroupNameException,
    open_tcp_port, get_security_group_by_id)
from deployment_helpers.constants import get_global_config, RABBIT_MQ_PORT
from deployment_helpers.general_utils import log

GLOBAL_CONFIGURATION = get_global_config()

RABBIT_MQ_SEC_GRP_DESCRIPTION = "allows connections to rabbitmq from servers with security group %s"
PROCESSING_MANAGER_NAME = "%s data processing manager"

####################################################################################################
######################################## Accessors #################################################
####################################################################################################

def get_instance_by_id(instance_id):
    ec2_client = create_ec2_client()
    return ec2_client.describe_instances(InstanceIds=[instance_id])['Reservations'][0]["Instances"][0]


def get_manager_private_ip(eb_environment_name):
    instance = get_manager_instance_by_eb_environment_name(eb_environment_name)
    return instance['NetworkInterfaces'][0]['PrivateIpAddress']


def get_manager_public_ip(eb_environment_name):
    """ returns a dictionary like the following
     {'PublicDnsName': 'ec2-18-216-26-40.us-east-2.compute.amazonaws.com',
      'PublicIp': '18.216.26.40'} """
    instance = get_manager_instance_by_eb_environment_name(eb_environment_name)
    return instance['NetworkInterfaces'][0]['PrivateIpAddresses'][0]['Association']['PublicIp']


def get_manager_instance_by_eb_environment_name(eb_environment_name):
    """ Get a manager dictionary of the currently running manager server. """
    managers = get_instances_by_name(PROCESSING_MANAGER_NAME % eb_environment_name)

    if len(managers) > 1:
        msg = "Discovered multiple manager servers. This configuration is not supported and should be corrected."
        log.error(msg)
        raise Exception(msg)

    if managers:
        return managers[0]
    else:
        log.warning("No manager found.")
        return None


def get_instances_by_name(instance_name):
    """ thank you to https://rob.salmond.ca/filtering-instances-by-name-with-boto3/ for having
    sufficient SEO to be a googleable answer on how to even do this.
    And then this stack overflow for how to query by instances that are running:
    https://stackoverflow.com/questions/37293366/what-is-the-correct-ways-to-write-boto3-filters-to-use-customise-tag-name
    """
    reservations = create_ec2_client().describe_instances(
            Filters=[
                {'Name': 'tag:Name',
                 'Values': [instance_name]},
                {'Name': 'instance-state-name',
                 'Values': ['running']},
            ]
    )['Reservations']

    # need to concatenate all instances from every "reservation", whatever that is.
    instances = []
    for reservation in reservations:
        instances.extend(reservation['Instances'])

    if not instances:
        log.error("Could not find any instances matching the name '%s'" % instance_name)

    return instances

####################################################################################################
######################################### Utilities ################################################
####################################################################################################

def get_most_recent_ubuntu():
    """ Unfortunately the different fundamental ec2 server types require a specific image type.
    All the ubuntu xenial prefixe matches are defined below, the currently selected is known to
    function for T2, M4, and C4 server classes.  Other server types may require testing the
    different classes
    """
    ec2_client = create_ec2_client()
    images = ec2_client.describe_images(
            Filters=[
                {"Name": 'state', "Values": ['available']},
                {"Name": 'name',
                 # "Values": ["ubuntu/images/ebs-ssd/ubuntu-xenial-16.04-amd64-server*"]}
                 # "Values": ["ubuntu/images/ubuntu-xenial-16.04-amd64-server*"]}
                 # "Values": ["ubuntu/images/hvm-instance/ubuntu-xenial-16.04-amd64-server*"]}
                "Values": ["ubuntu/images/hvm-ssd/ubuntu-xenial-16.04-amd64-server*"]}
            ]
    )['Images']
    # The names are time-sortable, we want the most recent one, it is at the bottom of a sorted list
    images.sort(key=lambda x: x['Name'])
    log.info("Using AMI '%s'" % images[-1]['Name'])
    return images[-1]

####################################################################################################
##################################### Security Groups ##############################################
####################################################################################################

def get_instance_security_group_by_eb_name(eb_environment_name):
    return get_rds_security_groups_by_eb_name(eb_environment_name)["instance_sec_grp"]


def construct_rabbit_mq_security_group_name(eb_environment_name):
    return eb_environment_name + "_allow_rabbit_mq_connections"


def get_or_create_rabbit_mq_security_group(eb_environment_name):
    rabbit_mq_sec_grp_name = construct_rabbit_mq_security_group_name(eb_environment_name)
    # we assume that the group was created correctly, don't attempt to add rules if we find it
    try:
        return get_security_group_by_name(rabbit_mq_sec_grp_name)
    except InvalidSecurityGroupNameException:
        log.info("Did not find a security group named '%s,' creating it." % rabbit_mq_sec_grp_name)
        instance_sec_grp_id = get_rds_security_groups_by_eb_name(eb_environment_name)["instance_sec_grp"]['GroupId']
        ingress_params = create_sec_grp_rule_parameters_allowing_traffic_from_another_security_group(
                tcp_port=RABBIT_MQ_PORT, sec_grp_id=instance_sec_grp_id
        )
        sec_grp = create_security_group(
                rabbit_mq_sec_grp_name,
                RABBIT_MQ_SEC_GRP_DESCRIPTION % instance_sec_grp_id,
                list_of_dicts_of_ingress_kwargs=[ingress_params]
        )
        open_tcp_port(sec_grp['GroupId'], 22)
        return get_security_group_by_id(sec_grp['GroupId'])

####################################################################################################
#################################### Instance Creation #############################################
####################################################################################################

def create_server(eb_environment_name, aws_server_type, security_groups=None):
    ec2_client = create_ec2_client()
    if security_groups is None:
        security_groups = []
    if not isinstance(security_groups, list):
        raise Exception("security_groups must be a list, received '%s'" % type(security_groups))

    ebs_parameters = {
        'DeviceName': '/dev/sda1',  # boot drive...
        'Ebs': {
            'DeleteOnTermination': True,
            'Encrypted': True,
            'VolumeSize': 8,
        # gigabytes, No storage is required on any ubuntu machines, 8 is default
            'VolumeType': 'gp2'}  # SSD...
    }

    instance = ec2_client.run_instances(
            ImageId=get_most_recent_ubuntu()['ImageId'],
            MinCount=1,
            MaxCount=1,
            KeyName=GLOBAL_CONFIGURATION['DEPLOYMENT_KEY_NAME'],
            InstanceType=aws_server_type,
            SecurityGroupIds=security_groups,
            # NetworkInterfaces=[{"DeviceIndex": 0,
            #                     "AssociatePublicIpAddress": True,
            #                     "SubnetId": config.public_subnet_id,
            #                     "Groups": security_groups_list}],
            # IamInstanceProfile={"Arn": MANAGER_IAM_ROLE},
            BlockDeviceMappings=[ebs_parameters],
            Monitoring={'Enabled': False},
            InstanceInitiatedShutdownBehavior='stop',
            # Placement={'AvailabilityZone': 'string',
            #            'Affinity': 'string',
            #            'GroupName': 'string',
            #            'HostId': 'string',
            #            'Tenancy': 'default'|'dedicated'|'host',
            #            'SpreadDomain': 'string'
            #           },
            # IamInstanceProfile={'Arn': 'string',
            #                    'Name': 'string'},

            # NetworkInterfaces=[ {
            #         'AssociatePublicIpAddress': True|False,
            #         'DeleteOnTermination': True|False,
            #         'Description': 'string',
            #         'DeviceIndex': 123,
            #         'Groups': ['string',],
            #         'Ipv6AddressCount': 123,
            #         'Ipv6Addresses': [ { 'Ipv6Address': 'string' }, ],
            #         'NetworkInterfaceId': 'string',
            #         'PrivateIpAddress': 'string',
            #         'PrivateIpAddresses': [ {'Primary': True|False,
            #                                  'PrivateIpAddress': 'string'},],
            #         'SecondaryPrivateIpAddressCount': 123,
            #         'SubnetId': 'string'
            #     }, ],
            #
            # TagSpecifications=[ {
            #         'ResourceType': 'customer-gateway'|'dhcp-options'|'image'|'instance'|'internet-gateway'|'network-acl'|'network-interface'|'reserved-instances'|'route-table'|'snapshot'|'spot-instances-request'|'subnet'|'security-group'|'volume'|'vpc'|'vpn-connection'|'vpn-gateway',
            #         'Tags': [ { 'Key': 'string',
            #                     'Value': 'string'},]
            #         },
            # ]
    )["Instances"][0]
    instance_id = instance["InstanceId"]
    instance_resource = create_ec2_resource().Instance(instance_id)
    log.info("Waiting for server %s to show up..." % instance_id)
    instance_resource.wait_until_exists()
    log.info("Waiting until server %s is up and running (this may take a minute) ..." % instance_id)
    instance_resource.wait_until_running()
    return get_instance_by_id(instance_id)


def create_processing_server(eb_environment_name, aws_server_type):
    instance_sec_grp_id = get_rds_security_groups_by_eb_name(eb_environment_name)["instance_sec_grp"]['GroupId']
    instance_info = create_server(eb_environment_name, aws_server_type,
                                  security_groups=[instance_sec_grp_id])
    instance_resource = create_ec2_resource().Instance(instance_info["InstanceId"])
    instance_resource.create_tags(Tags=[
        {"Key": "Name", "Value": "%s data processing server" % eb_environment_name},
        {"Key": "is_processing_worker", "Value": "1"}
    ])
    return instance_info


def create_processing_control_server(eb_environment_name, aws_server_type):
    """ The differences between a data processing worker server and a processing controller
    server is that the controller needs to allow connections from the processors. """

    get_rds_security_groups_by_eb_name(eb_environment_name)["instance_sec_grp"]['GroupId']

    # TODO: functions that terminate all worker and all manager servers for an environment

    manager_info = get_manager_instance_by_eb_environment_name(eb_environment_name)
    if manager_info is not None:
        if manager_info['InstanceType'] == aws_server_type:
            msg = "A manager server, %s, already exists for this environment, and it is of the provided type (%s)." % (manager_info['InstanceId'], aws_server_type)
        else:
            msg = "A manager server, %s, already exists for this environment." % manager_info['InstanceId']
        log.error(msg)
        msg = "You must terminate all worker and manager servers before you can create a new manager."
        log.error(msg)
        sleep(0.1)  # sometimes log has problems if you don't give it a second, the error messages above are critical
        raise Exception(msg)

    rabbit_mq_sec_grp_id = get_or_create_rabbit_mq_security_group(eb_environment_name)['GroupId']
    instance_sec_grp_id = get_rds_security_groups_by_eb_name(eb_environment_name)["instance_sec_grp"]['GroupId']

    try:
        open_tcp_port(instance_sec_grp_id, 22)
    except ClientError:
        # we need to open the ssh port for future worker servers, but it blows up with duplicate
        # if a user ever creates two managers during the life of the environment.
        pass

    instance_info = create_server(eb_environment_name, aws_server_type,
                                  security_groups=[rabbit_mq_sec_grp_id, instance_sec_grp_id])
    instance_resource = create_ec2_resource().Instance(instance_info["InstanceId"])
    instance_resource.create_tags(Tags=[
        {"Key": "Name", "Value":PROCESSING_MANAGER_NAME  % eb_environment_name},
        {"Key": "is_processing_manager", "Value":"1"}
    ])
    return instance_info
