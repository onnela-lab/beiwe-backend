from config.constants import ResearcherRole
from config.settings import DOMAIN_NAME
from database.study_models import Study
from database.user_models import Researcher, StudyRelation

# This component of pipeline is part of the Beiwe server, the correct import is 'from pipeline.xyz...'
from pipeline.boto_helpers import get_boto_client
from pipeline.configuration_getters import get_eb_config, get_generic_config


def refresh_data_access_credentials(freq, ssm_client=None, webserver=False):
    """
    Refresh the data access credentials for a particular BATCH USER user and upload them
    (encrypted) to the AWS Parameter Store. This enables AWS batch jobs to get the
    credentials and thereby access the data access API (DAA).
    :param freq: string, one of 'hourly' | 'daily' | 'weekly' | 'monthly' | 'manually'
    This is used to know what call the data access credentials on AWS.
    """
    
    # Get or create Researcher with no password. This means that nobody can log in as this
    # Researcher in the web interface.
    researcher_name = 'BATCH USER {}'.format(freq)
    mock_researchers = Researcher.objects.filter(username=researcher_name)
    if not mock_researchers.exists():
        mock_researcher = Researcher.create_without_password(researcher_name)
    else:
        mock_researcher = mock_researchers.get()
        mock_researcher.save()

    # Ensure that the Researcher is attached to all Studies. This allows them to access all
    # data via the DAA.
    for study in Study.objects.all():
        study.researchers.add(mock_researcher)
        StudyRelation.objects.get_or_create(
            study=study,
            researcher=mock_researcher,
            relationship=ResearcherRole.researcher
        )
    
    # Reset the credentials. This ensures that they aren't stale.
    access_key, secret_key = mock_researcher.reset_access_credentials()

    if not webserver:
        generic_config = get_generic_config()
    else:
        generic_config = get_eb_config()

    # Append the frequency to the SSM (AWS Systems Manager) names. This ensures that the
    # different frequency jobs' keys do not overwrite each other.
    access_key_ssm_name = '{}-{}'.format(generic_config['access_key_ssm_name'], freq)
    secret_key_ssm_name = '{}-{}'.format(generic_config['secret_key_ssm_name'], freq)

    # Put the credentials (encrypted) into AWS Parameter Store
    if not ssm_client:
        ssm_client = get_boto_client('ssm')
    ssm_client.put_parameter(
        Name=access_key_ssm_name,
        Value=access_key,
        Type='SecureString',
        Overwrite=True,
    )
    ssm_client.put_parameter(
        Name=secret_key_ssm_name,
        Value=secret_key,
        Type='SecureString',
        Overwrite=True,
    )


def create_one_job(freq, study, patient_id, client=None, webserver=False):
    """
    Create an AWS batch job
    The aws_object_names and client parameters are optional. They are provided in case
    that this function is run as part of a loop, to avoid an unnecessarily large number of
    file operations or API calls.
    :param freq: string e.g. 'daily', 'manually'
    :param a Study database object
    :param a string of a patient id
    :param client: a credentialed boto3 client or None
    
    config needs are the following: job_name, job_defn_name, queue_name
    """
    
    # Get the AWS parameters and client if not provided
    if not webserver:
        aws_object_names = get_generic_config()
    else:
        aws_object_names = get_eb_config()

    # requires region_name be defined.
    if client is None:
        client = get_boto_client('batch')

    client.submit_job(
        jobName=aws_object_names['job_name'].format(freq=freq),
        jobDefinition=aws_object_names['job_defn_name'],
        jobQueue=aws_object_names['queue_name'],
        containerOverrides={
            'environment': [
                {
                    'name': 'study_object_id',
                    'value': str(study.object_id),
                }, {
                    'name': 'study_name',
                    'value': Study.objects.get(object_id=study.object_id).name,
                }, {
                    'name': 'FREQ',
                    'value': freq,
                }, {
                    'name': 'patient_id',
                    'value': patient_id,
                },{
                    'name': 'server_url',
                    'value': DOMAIN_NAME,
                }

            ],
        },
    )


# TODO: these are not currently used at all except in a cron job.  Pipeline is being converted (for now)
# to be per-patient, not per-study.  The reason for this is because there is too much data
# to download per study, and the concept of calling different code for monthly, weekly etc.
# appears to have been discarded.  Currently only manual runs work.

# def create_all_jobs(freq):
#     """
#     Create one AWS batch job for each Study object
#     :param freq: string e.g. 'daily', 'monthly'
#     """
#
#     # TODO: Boto3 version 1.4.8 has AWS Batch Array Jobs, which are extremely useful for the
#     # task this function performs. We should switch to using them.
#
#     # Get new data access credentials for the user
#     # aws_object_names = get_aws_object_names()
#     refresh_data_access_credentials(freq)
#
#     # TODO: If there are issues with servers not getting spun up in time, make this a
#     # ThreadPool with random spacing over the course of 5-10 minutes.
#     error_sentry = make_error_sentry("data", tags={"pipeline_frequency": freq})
#     for study in Study.objects.filter(deleted=False):
#         with error_sentry:
#             # For each study, create a job
#             object_id = study.object_id
#             create_one_job(freq, object_id)


# def hourly():
#     create_all_jobs('hourly')
#
#
# def daily():
#     create_all_jobs('daily')
#
#
# def weekly():
#     create_all_jobs('weekly')
#
#
# def monthly():
#     create_all_jobs('monthly')
