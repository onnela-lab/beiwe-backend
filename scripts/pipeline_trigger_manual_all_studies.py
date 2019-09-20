from os.path import abspath as _abspath
from sys import path as _path

_one_folder_up = _abspath(__file__).rsplit('/', 2)[0]
_path.insert(1, _one_folder_up)

from config import load_django

from datetime import timedelta

from django.utils import timezone

from database.data_access_models import ChunkRegistry
from database.study_models import Study
from libs.sentry import make_error_sentry
from pipeline.boto_helpers import get_boto_client
from pipeline.configuration_getters import get_current_region
from pipeline.index import create_one_job, refresh_data_access_credentials

pipeline_region = get_current_region()
ssm_client = get_boto_client('ssm', pipeline_region)
error_sentry = make_error_sentry("data", tags={"pipeline_frequency": "manually"})
batch_client = get_boto_client('batch', pipeline_region)
yesterday = timezone.now() - timedelta(days=1)

refresh_data_access_credentials('manually', ssm_client=ssm_client, webserver=False)

################################################################################################
# if you are running this on an ubuntu machine you have to sudo apt-get -y install cloud-utils #
################################################################################################

for study in Study.objects.all():
    with error_sentry:
        # we only want to run the pipeline for data that has been uploaded, Report all errors to sentry
        for patient_id in ChunkRegistry.get_updated_users_for_study(study, yesterday):
            create_one_job('manually', study, patient_id, batch_client, webserver=False)
            print("creating job for", study.name)

# raise errors
if error_sentry.errors:
    error_sentry.raise_errors()
