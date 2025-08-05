# run ipython -i manage_shell.py for an interactive shell session with most of the useful functions
# imported.

# to suppress a message out of a required library we need to run this before importing paramiko.
import warnings
warnings.filterwarnings(action='ignore', module='.*paramiko.*')


from pprint import pprint
from deployment_helpers.configuration_utils import *
from deployment_helpers.general_utils import *
from deployment_helpers.aws.boto_helpers import *
from deployment_helpers.aws.elastic_beanstalk import *
from deployment_helpers.aws.elastic_compute_cloud import *
from deployment_helpers.aws.iam import *
from deployment_helpers.aws.rds import *
from deployment_helpers.aws.security_groups import *
from deployment_helpers.aws.s3 import *
warnings.resetwarnings()

print("")
log.info("validating AWS credentials and global configuration...")
# validate the global configuration file
if not all((are_aws_credentials_present(), is_global_configuration_valid())):
    EXIT(1)

log.info(f'Using Access Key {get_aws_credentials()["AWS_ACCESS_KEY_ID"]}')

batch_client = create_batch_client()
eb_client = create_eb_client()
ec2_client = create_ec2_client()
ec2_resource = create_ec2_resource()
iam_client = create_iam_client()
rds_client = create_rds_client()
s3_client = create_s3_client()
sts_client = create_sts_client()


data = sts_client.get_caller_identity()
user_id = data["UserId"]
account_id = data['Account']
user = iam_client.get_user()["User"]["UserName"]
aliases = iam_client.list_account_aliases()['AccountAliases']

log.info(f'This session is for account {account_id} and uses credentials for user {user_id} ({user}).')

if aliases:
    log.info(f'Account is also known by these aliases: {", ".join(aliases)}')
else:
    log.warning("Found no aliases for this account.")

del data, user_id, account_id, user, aliases
