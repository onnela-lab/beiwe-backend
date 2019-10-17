from deployment_helpers.configuration_utils import *
from deployment_helpers.general_utils import *
from deployment_helpers.aws.boto_helpers import *
from deployment_helpers.aws.elastic_beanstalk import *
from deployment_helpers.aws.elastic_compute_cloud import *
from deployment_helpers.aws.iam import *
from deployment_helpers.aws.rds import *
from deployment_helpers.aws.security_groups import *
from deployment_helpers.aws.s3 import *

print("")
log.info("validating AWS credentials and global configuration...")
# validate the global configuration file
if not all((are_aws_credentials_present(), is_global_configuration_valid())):
    EXIT(1)

eb_client = create_eb_client()
ec2_client = create_ec2_client()
ec2_resource = create_ec2_resource()
iam_client = create_iam_client()
rds_client = create_rds_client()
batch_client = create_batch_client()
s3_client = create_s3_client()
