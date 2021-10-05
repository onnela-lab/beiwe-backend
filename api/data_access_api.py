from datetime import datetime

from django.db.models import QuerySet
from flask import abort, Blueprint, json, request, Response

from authentication.data_access_authentication import api_study_credential_check, get_api_study
from config.constants import ALL_DATA_STREAMS, API_TIME_FORMAT
from database.data_access_models import ChunkRegistry, PipelineUpload
from database.user_models import Participant
from libs.streaming_zip import zip_generator, zip_generator_for_pipeline

data_access_api = Blueprint('data_access_api', __name__)

chunk_fields = ("pk", "participant_id", "data_type", "chunk_path", "time_bin", "chunk_hash",
                "participant__patient_id", "study_id", "survey_id", "survey__object_id")


@data_access_api.route("/get-participants/", methods=['POST', "GET"])
@api_study_credential_check(conditionally_block_test_studies=False)
def get_participants():
    """ This endpoint entirely uses the built-in api content, just provide the api keys and the
    study id or study pk. """
    participants = list(
        get_api_study()
        .participants.order_by("patient_id")
        .values_list("patient_id", flat=True)
    )
    return Response(json.dumps(participants), mimetype="json/text")


@data_access_api.route("/get-data/v1", methods=['POST', "GET"])
@api_study_credential_check(conditionally_block_test_studies=True)
def get_data():
    """ Required: access key, access secret, study_id
    JSON blobs: data streams, users - default to all
    Strings: date-start, date-end - format as "YYYY-MM-DDThh:mm:ss"
    optional: top-up = a file (registry.dat)
    cases handled:
        missing creds or study, invalid researcher or study, researcher does not have access
        researcher creds are invalid
        (Flask automatically returns a 400 response if a parameter is accessed
        but does not exist in request.values() )
    Returns a zip file of all data files found by the query. """

    query_args = {}
    determine_data_streams_for_db_query(query_args)
    determine_users_for_db_query(query_args)
    determine_time_range_for_db_query(query_args)

    # Do query! (this is actually a generator)
    get_these_files = handle_database_query(get_api_study().pk, query_args, registry_dict=parse_registry())

    # If the request is from the web form we need to indicate that it is an attachment,
    # and don't want to create a registry file.
    # Oddly, it is the presence of  mimetype=zip that causes the streaming response to actually stream.
    if 'web_form' in request.values:
        return Response(
            zip_generator(get_these_files, construct_registry=False),
            mimetype="zip",
            headers={'Content-Disposition': 'attachment; filename="data.zip"'}
        )
    else:
        return Response(
            zip_generator(get_these_files, construct_registry=True),
            mimetype="zip",
        )


@data_access_api.route("/get-pipeline/v1", methods=["GET", "POST"])
@api_study_credential_check()
def pipeline_data_download():
    # the following two cases are for difference in content wrapping between the CLI script and
    # the download page.
    study = get_api_study()
    if 'tags' in request.values:
        try:
            tags = json.loads(request.values['tags'])
        except ValueError:
            tags = request.form.getlist('tags')
        query = PipelineUpload.objects.filter(study__id=study.id, tags__tag__in=tags)
    else:
        query = PipelineUpload.objects.filter(study__id=study.id)

    return Response(
        zip_generator_for_pipeline(query),
        mimetype="zip",
        headers={'Content-Disposition': 'attachment; filename="data.zip"'}
    )


def parse_registry():
    """ Parses the provided registry.dat file and returns a dictionary of chunk
    file names and hashes.  (The registry file is just a json dictionary containing
    a list of file names and hashes.) """
    registry = request.values.get("registry", None)
    if registry is None:
        return None

    try:
        ret = json.loads(registry)
    except ValueError:
        return abort(400)

    if not isinstance(ret, dict):
        return abort(400)

    return ret


def str_to_datetime(time_string):
    """ Translates a time string to a datetime object, raises a 400 if the format is wrong."""
    try:
        return datetime.strptime(time_string, API_TIME_FORMAT)
    except ValueError as e:
        if "does not match format" in str(e):
            return abort(400)


#########################################################################################
############################ DB Query For Data Download #################################
#########################################################################################

def determine_data_streams_for_db_query(query_dict: dict):
    """ Determines, from the html request, the data streams that should go into the database query.
    Modifies the provided query object accordingly, there is no return value
    Throws a 404 if the data stream provided does not exist. """
    if 'data_streams' in request.values:
        # the following two cases are for difference in content wrapping between
        # the CLI script and the download page.
        try:
            query_dict['data_types'] = json.loads(request.values['data_streams'])
        except ValueError:
            query_dict['data_types'] = request.form.getlist('data_streams')

        for data_stream in query_dict['data_types']:
            if data_stream not in ALL_DATA_STREAMS:
                return abort(404)


def determine_users_for_db_query(query: dict):
    """ Determines, from the html request, the users that should go into the database query.
    Modifies the provided query object accordingly, there is no return value.
    Throws a 404 if a user provided does not exist. """
    if 'user_ids' in request.values:
        try:
            query['user_ids'] = [user for user in json.loads(request.values['user_ids'])]
        except ValueError:
            query['user_ids'] = request.form.getlist('user_ids')

        # Ensure that all user IDs are patient_ids of actual Participants
        if not Participant.objects.filter(patient_id__in=query['user_ids']).count() == len(query['user_ids']):
            return abort(404)


def determine_time_range_for_db_query(query: dict):
    """ Determines, from the html request, the time range that should go into the database query.
    Modifies the provided query object accordingly, there is no return value.
    Throws a 404 if a user provided does not exist. """
    if 'time_start' in request.values:
        query['start'] = str_to_datetime(request.values['time_start'])
    if 'time_end' in request.values:
        query['end'] = str_to_datetime(request.values['time_end'])


def handle_database_query(study_id: int, query_dict: dict, registry_dict: dict = None) -> QuerySet:
    """ Runs the database query and returns a QuerySet. """
    chunks = ChunkRegistry.get_chunks_time_range(study_id, **query_dict)

    if not registry_dict:
        return chunks.values(*chunk_fields)

    # If there is a registry, we need to filter on the chunks
    else:
        # Get all chunks whose path and hash are both in the registry
        possible_registered_chunks = (
            chunks
                .filter(chunk_path__in=registry_dict, chunk_hash__in=registry_dict.values())
                .values('pk', 'chunk_path', 'chunk_hash')
        )

        # determine those chunks that we do not want present in the download
        # (get a list of pks that have hashes that don't match the database)
        registered_chunk_pks = [
            c['pk'] for c in possible_registered_chunks
            if registry_dict[c['chunk_path']] == c['chunk_hash']
        ]

        # add the exclude and return the queryset
        return chunks.exclude(pk__in=registered_chunk_pks).values(*chunk_fields)
