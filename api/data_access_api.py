from __future__ import print_function

from multiprocessing.pool import ThreadPool
from zipfile import ZipFile, ZIP_STORED

from datetime import datetime
from flask import Blueprint, request, abort, json, Response

from config import load_django

from config.constants import (API_TIME_FORMAT, VOICE_RECORDING, ALL_DATA_STREAMS,
    SURVEY_ANSWERS, SURVEY_TIMINGS, IMAGE_FILE)
from database.models import is_object_id
from database.data_access_models import ChunkRegistry, PipelineRegistry
from database.study_models import Study
from database.user_models import Participant, Researcher, StudyRelation
from libs.s3 import s3_retrieve, s3_upload
from libs.streaming_bytes_io import StreamingBytesIO

from database.data_access_models import PipelineUpload, InvalidUploadParameterError, \
    PipelineUploadTags

# Data Notes
# The call log has the timestamp column as the 3rd column instead of the first.
# The Wifi and Identifiers have timestamp in the file name.
# The debug log has many lines without timestamps.

data_access_api = Blueprint('data_access_api', __name__)

#########################################################################################

def get_and_validate_study_id(chunked_download=False):
    """
    Checks for a valid study object id or primary key.
    If neither is given, a 400 (bad request) error is raised.
    Study object id malformed (not 24 characters) causes 400 error.
    Study object id otherwise invalid causes 400 error.
    Study does not exist in our database causes 404 error.
    """
    study = _get_study_or_abort_404(request.values.get('study_id', None),
                                    request.values.get('study_pk', None))

    try:
        # FIXME: this is an unsafe solution to identifying the exception to the raw data access rule
        # for the batch user tasks. Update the researcher model to have a special flag.
        r = Researcher.objects.get(access_key_id=request.values["access_key"])
        override_for_batch = r.username.startswith("BATCH USER")
    except Researcher.DoesNotExist:
        override_for_batch = False
        
    if not override_for_batch and not study.is_test and chunked_download:
        # You're only allowed to download chunked data from test studies
        print("study '%s' does not allow raw data download." % study.name)
        return abort(404)
    else:
        return study


def _get_study_or_abort_404(study_object_id, study_pk):
    if study_object_id:
        # If the ID is incorrectly sized, we return a 400
        if not is_object_id(study_object_id):
            print("Received invalid length objectid as study_id in the data access API.")
            return abort(400)
        # If no Study with the given ID exists, we return a 404
        try:
            study = Study.objects.get(object_id=study_object_id)
        except Study.DoesNotExist:
            print("study '%s' does not exist." % study_object_id)
            return abort(404)
        else:
            return study
    elif study_pk:
        # If no Study with the given ID exists, we return a 404
        try:
            study = Study.objects.get(pk=study_pk)
        except Study.DoesNotExist:
            print("study '%s' does not exist." % study_pk)
            return abort(404)
        else:
            return study
    else:
        return abort(400)


def get_and_validate_researcher(study):
    """
    Finds researcher based on the secret key provided.
    Returns 403 if researcher doesn't exist, is not credentialed on the study, or if
    the secret key does not match.
    """
    
    access_key_id = request.values["access_key"]
    access_secret = request.values["secret_key"]
    
    try:
        researcher = Researcher.objects.get(access_key_id=access_key_id)
    except Researcher.DoesNotExist:
        return abort(403)  # access key DNE
    
    if not StudyRelation.objects.filter(study_id=study.pk, researcher=researcher).exists():
        return abort(403)  # researcher is not credentialed for this study
    
    if not researcher.validate_access_credentials(access_secret):
        return abort(403)  # incorrect secret key
    
    return researcher


#########################################################################################

@data_access_api.route("/get-studies/v1", methods=['POST', "GET"])
def get_studies():
    """
    Retrieve a dict containing the object ID and name of all Study objects that the user can access
    If a GET request, access_key and secret_key must be provided in the URL as GET params. If
    a POST request (strongly preferred!), access_key and secret_key must be in the POST
    request body.
    :return: string: JSON-dumped dict {object_id: name}
    """
    
    # Get the access keys
    access_key = request.values["access_key"]
    access_secret = request.values["secret_key"]
    
    try:
        researcher = Researcher.objects.get(access_key_id=access_key)
    except Researcher.DoesNotExist:
        return abort(403)
    
    if not researcher.validate_access_credentials(access_secret):
        return abort(403)  # incorrect secret key

    return json.dumps(
        dict(StudyRelation.objects.filter(researcher=researcher).values_list("study__object_id", "study__name"))
    )


@data_access_api.route("/get-users/v1", methods=['POST', "GET"])
def get_users_in_study():

    study_object_id = request.values.get("study_id", "")
    # if not is_object_id(study_object_id):
    if not is_object_id(study_object_id):
        print("provided object id '%s' is not an object id" % study_object_id)
        return abort(404)
    
    try:
        study = Study.objects.get(object_id=study_object_id)
    except Study.DoesNotExist:
        print("study '%s' does not exist" % study_object_id)
        return abort(404)
    
    get_and_validate_researcher(study)
    return json.dumps(list(study.participants.values_list('patient_id', flat=True)))


@data_access_api.route("/get-data/v1", methods=['POST', "GET"])
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
    
    # uncomment the following line when doing a reindex
    # return abort(503)
    
    study = get_and_validate_study_id(chunked_download=True)
    get_and_validate_researcher(study)

    query = {}
    determine_data_streams_for_db_query(query)  # select data streams
    determine_users_for_db_query(query)  # select users
    determine_time_range_for_db_query(query)  # construct time ranges
    
    # Do query (this is actually a generator)
    if "registry" in request.values:
        get_these_files = handle_database_query(study.pk, query, registry=parse_registry(request.values["registry"]))
    else:
        get_these_files = handle_database_query(study.pk, query, registry=None)
    
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
                mimetype="zip"
        )


# from libs.security import generate_random_string

# Note: you cannot access the request context inside a generator function
def zip_generator(files_list, construct_registry=False):
    """ Pulls in data from S3 in a multithreaded network operation, constructs a zip file of that
    data. This is a generator, advantage is it starts returning data (file by file, but wrapped
    in zip compression) almost immediately. """
    
    processed_files = set()
    duplicate_files = set()
    pool = ThreadPool(3)
    # 3 Threads has been heuristically determined to be a good value, it does not cause the server
    # to be overloaded, and provides more-or-less the maximum data download speed.  This was tested
    # on an m4.large instance (dual core, 8GB of ram).
    file_registry = {}
    
    zip_output = StreamingBytesIO()
    zip_input = ZipFile(zip_output, mode="w", compression=ZIP_STORED, allowZip64=True)
    # random_id = generate_random_string()[:32]
    # print "returning data for query %s" % random_id
    try:
        # chunks_and_content is a list of tuples, of the chunk and the content of the file.
        # chunksize (which is a keyword argument of imap, not to be confused with Beiwe Chunks)
        # is the size of the batches that are handed to the pool. We always want to add the next
        # file to retrieve to the pool asap, so we want a chunk size of 1.
        # (In the documentation there are comments about the timeout, it is irrelevant under this construction.)
        chunks_and_content = pool.imap_unordered(batch_retrieve_s3, files_list, chunksize=1)
        total_size = 0
        for chunk, file_contents in chunks_and_content:
            if construct_registry:
                file_registry[chunk['chunk_path']] = chunk["chunk_hash"]
            file_name = determine_file_name(chunk)
            if file_name in processed_files:
                duplicate_files.add((file_name, chunk['chunk_path']))
                continue
            processed_files.add(file_name)
            # print file_name
            zip_input.writestr(file_name, file_contents)
            # These can be large, and we don't want them sticking around in memory as we wait for the yield
            del file_contents, chunk
            # print len(zip_output)
            x = zip_output.getvalue()
            total_size += len(x)
            # print "%s: %sK, %sM" % (random_id, total_size / 1024, total_size / 1024 / 1024)
            yield x  # yield the (compressed) file information
            del x
            zip_output.empty()
        
        if construct_registry:
            zip_input.writestr("registry", json.dumps(file_registry))
        
        # close, then yield all remaining data in the zip.
        zip_input.close()
        yield zip_output.getvalue()
    
    except None:
        # The try-except-finally block is here to guarantee the Threadpool is closed and terminated.
        # we don't handle any errors, we just re-raise any error that shows up.
        # (with statement does not work.)
        raise
    finally:
        # We rely on the finally block to ensure that the threadpool will be closed and terminated,
        # and also to print an error to the log if we need to.
        pool.close()
        pool.terminate()
        if duplicate_files:
            duplcate_file_message = "encountered duplicate files: %s" % ",".join(
                    str(name_path) for name_path in duplicate_files)


#########################################################################################

def parse_registry(reg_dat):
    """ Parses the provided registry.dat file and returns a dictionary of chunk
    file names and hashes.  (The registry file is just a json dictionary containing
    a list of file names and hashes.) """
    try:
        ret = json.loads(reg_dat)
    except ValueError:
        return abort(400)
    if not isinstance(ret, dict):
        return abort(400)
    return ret


def determine_file_name(chunk):
    """ Generates the correct file name to provide the file with in the zip file.
        (This also includes the folder location files in the zip.) """
    extension = chunk["chunk_path"][-3:]  # get 3 letter file extension from the source.
    if chunk["data_type"] == SURVEY_ANSWERS:
        # add the survey_id from the file path.
        return "%s/%s/%s/%s.%s" % (chunk["participant__patient_id"], chunk["data_type"],
                                   chunk["chunk_path"].rsplit("/", 2)[1], # this is the survey id
                                   str(chunk["time_bin"]).replace(":", "_"), extension)
    
    elif chunk["data_type"] == IMAGE_FILE:
        # add the survey_id from the file path.
        return "%s/%s/%s/%s/%s" % (
            chunk["participant__patient_id"],
            chunk["data_type"],
            chunk["chunk_path"].rsplit("/", 3)[1], # this is the survey id
            chunk["chunk_path"].rsplit("/", 2)[1], # this is the instance of the user taking a survey
            chunk["chunk_path"].rsplit("/", 1)[1]
        )
    
    elif chunk["data_type"] == SURVEY_TIMINGS:
        # add the survey_id from the database entry.
        return "%s/%s/%s/%s.%s" % (chunk["participant__patient_id"], chunk["data_type"],
                                   chunk["survey__object_id"],  # this is the survey id
                                   str(chunk["time_bin"]).replace(":", "_"), extension)
    
    elif chunk["data_type"] == VOICE_RECORDING:
        # Due to a bug that was not noticed until July 2016 audio surveys did not have the survey id
        # that they were associated with.  Later versions of the app (legacy update 1 and Android 6)
        # correct this.  We can identify those files by checking for the existence of the extra /.
        # When we don't find it, we revert to original behavior.
        if chunk["chunk_path"].count("/") == 4:  #
            return "%s/%s/%s/%s.%s" % (chunk["participant__patient_id"], chunk["data_type"],
                                       chunk["chunk_path"].rsplit("/", 2)[1],  # this is the survey id
                                       str(chunk["time_bin"]).replace(":", "_"), extension)
    
    # all other files have this form:
    return "%s/%s/%s.%s" % (chunk['participant__patient_id'], chunk["data_type"],
                            str(chunk["time_bin"]).replace(":", "_"), extension)


def str_to_datetime(time_string):
    """ Translates a time string to a datetime object, raises a 400 if the format is wrong."""
    try:
        return datetime.strptime(time_string, API_TIME_FORMAT)
    except ValueError as e:
        if "does not match format" in e.message:
            return abort(400)


def batch_retrieve_s3(chunk):
    """ Data is returned in the form (chunk_object, file_data). """
    return chunk, s3_retrieve(chunk["chunk_path"],
                              study_object_id=Study.objects.get(id=chunk["study_id"]).object_id,
                              raw_path=True)


#########################################################################################
################################### DB Query ############################################
#########################################################################################

def determine_data_streams_for_db_query(query):
    """ Determines, from the html request, the data streams that should go into the database query.
    Modifies the provided query object accordingly, there is no return value
    Throws a 404 if the data stream provided does not exist.
    :param query: expects a dictionary object. """
    if 'data_streams' in request.values:
        # the following two cases are for difference in content wrapping between
        # the CLI script and the download page.
        try:
            query['data_types'] = json.loads(request.values['data_streams'])
        except ValueError:
            query['data_types'] = request.form.getlist('data_streams')
        
        for data_stream in query['data_types']:
            if data_stream not in ALL_DATA_STREAMS:
                print("data stream '%s' is invalid" % data_stream)
                return abort(404)


def determine_users_for_db_query(query):
    """ Determines, from the html request, the users that should go into the database query.
    Modifies the provided query object accordingly, there is no return value.
    Throws a 404 if a user provided does not exist.
    :param query: expects a dictionary object. """
    if 'user_ids' in request.values:
        try:
            query['user_ids'] = [user for user in json.loads(request.values['user_ids'])]
        except ValueError:
            query['user_ids'] = request.form.getlist('user_ids')
        
        # Ensure that all user IDs are patient_ids of actual Participants
        if not Participant.objects.filter(patient_id__in=query['user_ids']).count() == len(query['user_ids']):
            print("invalid user ids: %s" % query['user_ids'])
            return abort(404)


def determine_time_range_for_db_query(query):
    """ Determines, from the html request, the time range that should go into the database query.
    Modifies the provided query object accordingly, there is no return value.
    Throws a 404 if a user provided does not exist.
    :param query: expects a dictionary object. """
    if 'time_start' in request.values:
        query['start'] = str_to_datetime(request.values['time_start'])
    if 'time_end' in request.values:
        query['end'] = str_to_datetime(request.values['time_end'])


def handle_database_query(study_id, query, registry=None):
    """
    Runs the database query and returns a QuerySet.
    """
    chunk_fields = ["pk", "participant_id", "data_type", "chunk_path", "time_bin", "chunk_hash",
                    "participant__patient_id", "study_id", "survey_id", "survey__object_id"]

    chunks = ChunkRegistry.get_chunks_time_range(study_id, **query)
    
    if not registry:
        return chunks.values(*chunk_fields)
    
    # If there is a registry, we need to filter the chunks
    else:
        # Get all chunks whose path and hash are both in the registry
        possible_registered_chunks = (
            chunks
            .filter(chunk_path__in=registry, chunk_hash__in=registry.values())
            .values('pk', 'chunk_path', 'chunk_hash')
        )
        
        # determine those chunks that we do not want present in the download
        # (get a list of pks that have hashes that don't match the database)
        registered_chunk_pks = [
            c['pk'] for c in possible_registered_chunks if registry[c['chunk_path']] == c['chunk_hash']
        ]
        
        # add the exclude and return the queryset
        unregistered_chunks = chunks.exclude(pk__in=registered_chunk_pks)
        return unregistered_chunks.values(*chunk_fields)


#########################################################################################
################################### Pipeline ############################################
#########################################################################################

VALID_PIPELINE_POST_PARAMS = PipelineUpload.REQUIREDS
VALID_PIPELINE_POST_PARAMS.append("access_key")
VALID_PIPELINE_POST_PARAMS.append("secret_key")

# before reenabling, audio filenames on s3 were incorrectly enforced to have millisecond
# precision, remove trailing zeros this does not affect data downloading because those file times
    #  are generated from the chunk registry
@data_access_api.route("/pipeline-upload/v1", methods=['POST', 'GET'])
def data_pipeline_upload():
    #Cases: invalid access creds
    access_key = request.values["access_key"]
    access_secret = request.values["secret_key"]

    if not Researcher.objects.filter(access_key_id=access_key).exists():
        return abort(403) # access key DNE
    researcher = Researcher.objects.get(access_key_id=access_key)
    if not researcher.validate_access_credentials(access_secret):
        return abort(403)  # incorrect secret key
    # case: invalid study
    study_id = request.values["study_id"]

    if not Study.objects.filter(object_id=study_id).exists():
        return abort(404)

    study_obj = Study.objects.get(object_id=study_id)

    # case: study not authorized for user
    if not study_obj.get_researchers().filter(id=researcher.id).exists():
        return abort(403)

    # block extra keys
    errors = []
    for key in request.values.keys():
        if key not in VALID_PIPELINE_POST_PARAMS:
            errors.append('encountered invalid parameter: "%s"' % key)
    
    if errors:
        return Response("\n".join(errors), 400)
        
    try:
        creation_args, tags = PipelineUpload.get_creation_arguments(request.values, request.files['file'])
    except InvalidUploadParameterError as e:
        return Response(e.message, 400)
    s3_upload(
            creation_args['s3_path'],
            request.files['file'].read(),
            Study.objects.get(id=creation_args['study_id']).object_id,
            raw_path=True
    )

    pipeline_upload = PipelineUpload(object_id=PipelineUpload.generate_objectid_string('object_id'), **creation_args)
    pipeline_upload.save()

    for tag in tags:
        pipeline_upload_tag = PipelineUploadTags(pipeline_upload=pipeline_upload, tag=tag)
        pipeline_upload_tag.save()

    return Response("SUCCESS", status=200)


@data_access_api.route("/pipeline-json-upload/v1", methods=['POST'])
def json_pipeline_upload():
    access_key = request.values["access_key"]
    access_secret = request.values["secret_key"]

    if not Researcher.objects.filter(access_key_id=access_key).exists():
        return abort(403)  # access key DNE
    researcher = Researcher.objects.get(access_key_id=access_key)
    if not researcher.validate_access_credentials(access_secret):
        return abort(403)  # incorrect secret key

    # case: invalid study
    study_id = request.values["study_id"]
    if not Study.objects.filter(object_id=study_id).exists():
        return abort(404)

    study_obj = Study.objects.get(object_id=study_id)
    # case: study not authorized for user
    if not study_obj.get_researchers().filter(id=researcher.id).exists():
        return abort(403)

    json_data = request.values.get("summary_output", None)
    file_name = request.values.get("file_name", None)
    patient_id = request.values.get("patient_id", None)
    participant_id = Participant.objects.get(patient_id=patient_id).id

    if json_data is None:
        raise Exception("json_data")
    if file_name is None:
        raise Exception("summary_type")
    if patient_id is None:
        raise Exception("patient_id")
    if participant_id is None:
        raise Exception("participant_id")

    if "gps_summaries" in file_name:
        summary_type = "gps_summary"
    elif "powerstate_summary" in file_name:
        summary_type = "powerstate_summary"
    elif "text_summary" in file_name:
        summary_type = "text_summary"
    elif "call_summary" in file_name:
        summary_type = "call_summary"
    else:
        summary_type = file_name

    PipelineRegistry.register_pipeline_data(study_obj, participant_id, json_data, summary_type)
    return Response("SUCCESS", status=200)


@data_access_api.route("/get-pipeline/v1", methods=["GET", "POST"])
def pipeline_data_download():
    study_obj = get_and_validate_study_id(chunked_download=False)
    get_and_validate_researcher(study_obj)

    # the following two cases are for difference in content wrapping between the CLI script and
    # the download page.
    if 'tags' in request.values:
        try:
            tags = json.loads(request.values['tags'])
        except ValueError:
            tags = request.form.getlist('tags')

        query = PipelineUpload.objects.filter(study__id=study_obj.id, tags__tag__in=tags)
        
    else:
        query = PipelineUpload.objects.filter(study__id=study_obj.id)
    
    ####################################
    return Response(
            zip_generator_for_pipeline(query),
            mimetype="zip",
            headers={'Content-Disposition': 'attachment; filename="data.zip"'}
    )
    
#TODO: This is a trivial rewrite of the other zip generator function for minor differences. refactor when you get to django.
def zip_generator_for_pipeline(files_list):
    pool = ThreadPool(3)
    zip_output = StreamingBytesIO()
    zip_input = ZipFile(zip_output, mode="w", compression=ZIP_STORED, allowZip64=True)
    try:
        # chunks_and_content is a list of tuples, of the chunk and the content of the file.
        # chunksize (which is a keyword argument of imap, not to be confused with Beiwe Chunks)
        # is the size of the batches that are handed to the pool. We always want to add the next
        # file to retrieve to the pool asap, so we want a chunk size of 1.
        # (In the documentation there are comments about the timeout, it is irrelevant under this construction.)
        chunks_and_content = pool.imap_unordered(batch_retrieve_pipeline_s3, files_list, chunksize=1)
        for pipeline_upload, file_contents in chunks_and_content:
            # file_name = determine_file_name(chunk)
            zip_input.writestr("data/" + pipeline_upload.file_name, file_contents)
            # These can be large, and we don't want them sticking around in memory as we wait for the yield
            del file_contents, pipeline_upload
            yield zip_output.getvalue()  # yield the (compressed) file information
            zip_output.empty()
        
        # close, then yield all remaining data in the zip.
        zip_input.close()
        yield zip_output.getvalue()
    
    except None:
        # The try-except-finally block is here to guarantee the Threadpool is closed and terminated.
        # we don't handle any errors, we just re-raise any error that shows up.
        # (with statement does not work.)
        raise
    finally:
        # We rely on the finally block to ensure that the threadpool will be closed and terminated,
        # and also to print an error to the log if we need to.
        pool.close()
        pool.terminate()
        
        
def batch_retrieve_pipeline_s3(pipeline_upload):
    """ Data is returned in the form (chunk_object, file_data). """
    study = Study.objects.get(id = pipeline_upload.study_id)
    return pipeline_upload, s3_retrieve(pipeline_upload.s3_path,
                                        study.object_id,
                                        raw_path=True)


# class dummy_threadpool():
#     def imap_unordered(self, *args, **kwargs): #the existance of that self variable is key
#         # we actually want to cut off any threadpool args, which is conveniently easy because map does not use kwargs!
#         return map(*args)
#     def terminate(self): pass
#     def close(self): pass
