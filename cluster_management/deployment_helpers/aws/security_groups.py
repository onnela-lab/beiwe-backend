"""
celery workers need to connect to rabbitmq, postgres
postgres needs to allow rabbitmq server, celery workers, eb servers
rabbitmq needs to allow workers
"""
from botocore.exceptions import ClientError

from deployment_helpers.aws.boto_helpers import create_ec2_resource, create_ec2_client
from deployment_helpers.constants import get_global_config
from deployment_helpers.general_utils import log

GLOBAL_CONFIGURATION = get_global_config()

class InvalidSecurityGroupIdException(Exception): pass
class InvalidSecurityGroupNameException(Exception): pass

def get_security_group_by_id(sec_grp_id):
    try:
        return create_ec2_client().describe_security_groups(GroupIds=[sec_grp_id])['SecurityGroups'][0]
    except ClientError as e:
        # Boto3 throws unimportable errors.
        if "Invalid id:" in str(e):
            log.debug(str(e))
            raise InvalidSecurityGroupIdException(sec_grp_id)
        raise


def get_security_group_by_name(sec_grp_name):
    try:
        return create_ec2_client().describe_security_groups(GroupNames=[sec_grp_name])['SecurityGroups'][0]
    except ClientError as e:
        # Boto3 throws unimportable errors.
        if "InvalidGroup.NotFound" in str(e):
            log.debug(str(e))
            raise InvalidSecurityGroupNameException(sec_grp_name)
        raise


def create_sec_grp_rule_parameters_allowing_traffic_from_another_security_group(
        tcp_port, sec_grp_id, protocol="TCP"):
    # Do not even try to construct an inline declaration of security group rules.  They are simply
    # unpythonic, bloated, and really hard to read.  In addition errors are displayed like this:
    #   Unknown parameter in IpPermissions[0].UserIdGroupPairs[0]: "ToPort", must be one of:
    #   Description, GroupId, GroupName, PeeringStatus, UserId, VpcId, VpcPeeringConnectionId
    # The "mutation style" declaration is easier to read, harder to screw up the various [] and {}
    # nestings, easier to parse when looking at the boto error messages in development,
    # has significantly reduced line count,
    ingress_rule = {}
    ingress_rule["IpPermissions"] = [{}]
    ingress_rule["IpPermissions"][0]["FromPort"] = tcp_port
    ingress_rule["IpPermissions"][0]["ToPort"] = tcp_port
    ingress_rule["IpPermissions"][0]["IpProtocol"] = protocol
    ingress_rule["IpPermissions"][0]['UserIdGroupPairs'] = [{}]
    ingress_rule["IpPermissions"][0]['UserIdGroupPairs'][0]['GroupId'] = sec_grp_id
    return ingress_rule


def open_tcp_port(sec_grp_id, port, ip_address="0.0.0.0/0"):
    port = int(port)
    create_ec2_client().authorize_security_group_ingress(
            GroupId=sec_grp_id, IpProtocol="tcp", CidrIp=ip_address, FromPort=port, ToPort=port)


def create_security_group(group_name, description,
                          list_of_dicts_of_ingress_kwargs=None, list_of_dicts_of_egress_kwargs=None):
    """
    mostly kwargs should look like this: ToPort=22, IpProtocol="TCP", SourceSecurityGroupName="thing"
    """
    if list_of_dicts_of_ingress_kwargs is None:
        list_of_dicts_of_ingress_kwargs = []
    if list_of_dicts_of_egress_kwargs is None:
        list_of_dicts_of_egress_kwargs = []

    if not isinstance(list_of_dicts_of_ingress_kwargs, list):
        raise Exception("list_of_dicts_of_ingress_kwargs was not a list, it was a %s" % type(list_of_dicts_of_ingress_kwargs))
    if not isinstance(list_of_dicts_of_egress_kwargs, list):
        raise Exception("list_of_dicts_of_egress_kwargs was not a list, it was a %s" % type(list_of_dicts_of_egress_kwargs))
    
    ec2_client = create_ec2_client()
    ec2_resource = create_ec2_resource()
    
    sec_grp = ec2_client.create_security_group(
            VpcId=GLOBAL_CONFIGURATION["VPC_ID"],
            GroupName=group_name,
            Description=description,
    )
    sec_grp_resource = ec2_resource.SecurityGroup(sec_grp["GroupId"])
    
    for ingress_params in list_of_dicts_of_ingress_kwargs:
        sec_grp_resource.authorize_ingress(**ingress_params)
        
    for egress_params in list_of_dicts_of_egress_kwargs:
        sec_grp_resource.authorize_egress(**egress_params)

    return get_security_group_by_id(sec_grp["GroupId"])
