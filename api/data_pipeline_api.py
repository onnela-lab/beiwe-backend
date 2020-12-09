from flask import Blueprint, flash, redirect

from authentication.admin_authentication import authenticate_researcher_study_access
from database.study_models import Study
from libs.sentry import make_error_sentry
from pipeline.boto_helpers import get_boto_client
from pipeline.configuration_getters import get_current_region
from pipeline.index import create_one_job, refresh_data_access_credentials

data_pipeline_api = Blueprint('data_pipeline_api', __name__)


@data_pipeline_api.route('/run-manual-code/<string:study_id>', methods=['POST'])
@authenticate_researcher_study_access
def run_manual_code(study_id):
    """
    Create an AWS Batch job for the Study specified
    :param study_id: Primary key of a Study
    """
    # we assume that the cluster is configured only in one region.
    pipeline_region = get_current_region()

    # Get the object ID of the study, used in the pipeline
    study = Study.objects.get(pk=study_id)

    error_sentry = make_error_sentry("data", tags={"pipeline_frequency": "manually"})
    # Get new data access credentials for the manual user, submit a manual job, display message
    # Report all errors to sentry including DataPipelineNotConfigured errors.
    with error_sentry:
        ssm_client = get_boto_client('ssm', pipeline_region)
        refresh_data_access_credentials('manually', ssm_client=ssm_client, webserver=True)
        batch_client = get_boto_client('batch', pipeline_region)
        for patient_id in study.participants.values_list("patient_id", flat=True):
            create_one_job('manually', study, patient_id, batch_client, webserver=True)
        flash('Data pipeline code successfully initiated!', 'success')
    
    if error_sentry.errors:
        flash('An unknown error occurred when trying to run this task.', category='danger')
    
    return redirect('/data-pipeline/{:s}'.format(study_id))
