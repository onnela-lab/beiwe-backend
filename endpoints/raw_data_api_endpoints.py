from __future__ import annotations

import json
from base64 import encodebytes as b64_encodebytes
from datetime import datetime
from typing import Generator, Iterable

import orjson
from dateutil import tz
from django.db import transaction
from django.db.models import CharField, OuterRef, QuerySet, Subquery, Value
from django.db.models.functions import Concat
from django.http.response import FileResponse
from django.utils import timezone
from django.utils.timezone import make_aware
from django.views.decorators.http import require_http_methods

from authentication.data_access_authentication import (api_study_credential_check,
    ApiStudyResearcherRequest)
from constants.common_constants import API_TIME_FORMAT
from constants.data_stream_constants import ALL_DATA_STREAMS, SURVEY_ANSWERS
from constants.raw_data_constants import CHUNK_FIELDS
from data_access_api_reference.download_data import VOICE_RECORDING
from database.models import ChunkRegistry, DataAccessRecord, Participant, S3File, Study
from libs.streaming_zip import ZipGenerator
from middleware.abort_middleware import abort


ENABLE_DATA_API_DEBUG = False


def log(*args, **kwargs):
    if ENABLE_DATA_API_DEBUG:
        print(*args, **kwargs)


@require_http_methods(['POST', "GET"])
@api_study_credential_check()
@transaction.non_atomic_requests
def get_data_v1(request: ApiStudyResearcherRequest):
    return _get_data(request, as_compressed=False)


@require_http_methods(['POST', "GET"])
@api_study_credential_check()
@transaction.non_atomic_requests
def get_data_v2_compressed(request: ApiStudyResearcherRequest):
    return _get_data(request, as_compressed=True)


def _get_data(request: ApiStudyResearcherRequest, as_compressed: bool):
    """ Required: access key, access secret, study_id
    JSON blobs: data streams, users - default to all
    Strings: date-start, date-end - format as "YYYY-MM-DDThh:mm:ss"
    optional: top-up = a file (registry.dat)
    cases handled:
        missing credentials or study, invalid researcher or study, researcher does not have access
        researcher credentials are invalid
    Returns a zip file of all data files found by the query. """
    query_args = {}
    
    try:
        determine_data_streams_for_db_query(request, query_args)
        determine_users_for_db_query(request, query_args)
        determine_time_range_for_db_query(request, query_args)
        registry_dict = parse_registry(request)
    except Exception as e:
        post = dict(request.POST)
        post["access_key"] = post["secret_key"] = "sanitized"  # guaranteed to be present
        DataAccessRecord.objects.create(
            researcher=request.api_researcher,
            username=request.api_researcher.username,
            query_params=orjson.dumps(post).decode(),
            error="did not pass query validation, " + str(e),
        )
        raise
    
    # Do query! (this is actually a generator, it can only be iterated over once)
    get_these_files = handle_database_query(
        request.api_study, query_args, registry_dict=registry_dict
    )
    
    # make a record of the query, we are only tracking queries that make it to this point
    query_args["study_pk"] = request.api_study.pk  # add the study pk
    record = DataAccessRecord.objects.create(
        researcher=request.api_researcher,
        query_params=orjson.dumps(query_args).decode(),
        registry_dict_size=len(registry_dict) if registry_dict else 0,
        username=request.api_researcher.username,
    )
    
    streaming_zip_file = ZipGenerator(
        study=request.api_study,
        files_list=get_these_files,
        construct_registry='web_form' not in request.POST,
        threads=5,
        as_compressed=as_compressed,
    )
    try:
        streaming_response = FileResponse(
            streaming_zip_file,
            content_type="application/zip",
            as_attachment='web_form' in request.POST,
            filename="data.zip",
        )
        # for unknown reasons this call never happens in django's responding process, and so the
        # headers, which includes the file name, are never set.
        streaming_response.set_headers(None)
        return streaming_response
    except Exception as e:
        record.update_only(internal_error=True, error=str(e), bytes=streaming_zip_file.total_bytes)
    finally:
        # this runs when we return, not when the entire thing is done. have to reach in to request
        # and identify that the request is done.....
        record.update_only(time_end=timezone.now(), bytes=streaming_zip_file.total_bytes)
        # streaming_zip_file.stop()


def parse_registry(request: ApiStudyResearcherRequest) -> dict[str, str] | None:
    """ Parses the provided registry.dat file and returns a dictionary of chunk file names and hashes.
    (The registry file is just a json dictionary containing a list of file names and hashes.) """
    registry = request.POST.get("registry", None)
    if registry is None:
        log("no registry")
        return None
    
    try:
        ret = json.loads(registry)
    except ValueError:
        log("bad json registry")
        return abort(400, "bad registry")
    
    if not isinstance(ret, dict):
        log("json was not a dict")
        return abort(400, "bad registry dict")
    
    return ret


def str_to_datetime(time_string: str) -> datetime:
    """ Translates a time string to a datetime object, raises a 400 if the format is wrong."""
    try:
        return make_aware(datetime.strptime(time_string, API_TIME_FORMAT), tz.UTC)
    except ValueError as e:
        if "does not match format" in str(e):
            log("does not match format")
            log(str(e))
            return abort(400)
        raise  # not best practice but I'm okay with a potential 500 error alerting us to new cases


#########################################################################################
############################ DB Query For Data Download #################################
#########################################################################################


def determine_data_streams_for_db_query(request: ApiStudyResearcherRequest, query_dict: dict):
    """ Determines, from the html request, the data streams that should go into the database query.
    Modifies the provided query object accordingly, there is no return value
    Throws a 404 if the data stream provided does not exist. """
    if 'data_streams' in request.POST:
        # the following two cases are for difference in content wrapping between
        # the CLI script and the download page.
        try:
            query_dict['data_types'] = json.loads(request.POST['data_streams'])
        except ValueError:
            log("did not receive json data streams")
            query_dict['data_types'] = request.POST.getlist('data_streams')
        
        for data_stream in query_dict['data_types']:
            if data_stream not in ALL_DATA_STREAMS:
                log("invalid data stream:", data_stream)
                return abort(404, "bad data stream")


def determine_users_for_db_query(request: ApiStudyResearcherRequest, query: dict) -> None:
    """ Determines, from the html request, the users that should go into the database query.
    Modifies the provided query object accordingly, there is no return value.
    Throws a 404 if a user provided does not exist. """
    
    # "user_ids" is misnamed, we keep it for backwards compatibility, we have added participant_ids
    # and will prefer it when both are present.  Mano will eventually use participant_ids only.
    if 'participant_ids' in request.POST:
        _determine_users_for_db_query(request, query, 'participant_ids')
    elif 'user_ids' in request.POST:
        _determine_users_for_db_query(request, query, 'user_ids')


def _determine_users_for_db_query(request: ApiStudyResearcherRequest, query: dict, key: str):
    # the internal query is passed to a function with the misnamed key 'user_ids'
    try:
        try:
            query['participant_ids'] = list(json.loads(request.POST[key]))
        except ValueError:
            query['participant_ids'] = request.POST.getlist(key)
    except Exception:
        return abort(400, "bad patient id")
    
    # Ensure that all participant ids are real participants
    if Participant.objects.filter(patient_id__in=query['participant_ids']).count() != len(query['participant_ids']):
        log("invalid participant")
        return abort(404, "bad patient id")


def determine_time_range_for_db_query(request: ApiStudyResearcherRequest, query: dict):
    """ Determines, from the html request, the time range that should go into the database query.
    Modifies the provided query object accordingly, there is no return value. """
    if 'time_start' in request.POST:
        query['start'] = str_to_datetime(request.POST['time_start'])
    if 'time_end' in request.POST:
        query['end'] = str_to_datetime(request.POST['time_end'])


def handle_database_query(study: Study, query_params: dict, registry_dict: dict[str, str] = None) -> Iterable:
    """ Runs the database query and returns a QuerySet. """
    
    chunks = ChunkRegistry.get_chunks_time_range(study.id, **query_params)
    # the simple case where there isn't a registry uploaded
    if not registry_dict:
        log("no registry dict")
        return chunks.values(*CHUNK_FIELDS).iterator()
    log("filtering by registry dict of size", len(registry_dict))
    chunk_values_with_extras = combined_chunk_query(chunks, CHUNK_FIELDS)
    return filter_chunks_by_registry(chunk_values_with_extras, registry_dict)


def filter_chunks_by_registry(
    chunk_values_with_extras: QuerySet, registry_dict: dict[str, str]
) -> Generator:
    """ Consumes a registry dictiontary from a download request and a filters by hash the results
    of the query set.  This function is a generater that ~slowly yields chunk dicts that will then
    download files off S3, usually over in the ZipGenerator. """
    
    # ~Bug: requesters lack filenames with in millisecond precision, so they cannot reconstruct the
    # file paths for survey answers and audio recordings.
    # - for audio recordings we can just compare against all hashes in the registry. Easy.
    # - survey answers cannot do this because they can be identical between runs of the same survey.
    # - survey answers also can't do a prefix match because the names can be just too similar.
    
    # Dev decision: this is impossible to fix here, the correct solution is to have an api that
    # receives a data dump of the state from the server and compares locally. In fact I've built it.
    # It took 5 minutes.
    
    # keys: pk, participant_id, data_type, chunk_path, time_bin, chunk_hash,
    #   participant__patient_id, study_id, survey_id, survey__object_id
    registry_hashes: set[str] = {d.strip() for d in registry_dict.values()}
    for chunkdata in chunk_values_with_extras.iterator():
        
        # a 20 byte sha1 hash of the file contents sourced from the S3File table
        sha1 = chunkdata.pop("sha1")  # note: sha1 has a new line at the end
        data_stream = chunkdata["data_type"]
        
        if sha1 is None:  # don't bother checking anything if there is no hash
            print("no sha1 found for chunk:", chunkdata["chunk_path"])
            chunkdata["chunk_hash"] = None
            yield chunkdata
            continue
        
        if data_stream == SURVEY_ANSWERS:  # always yield survey answers 
            yield chunkdata
            continue
        
        chunkdata["chunk_hash"] = (chunk_hash := b64_encodebytes(sha1).decode())
        
        # skip audio files that match anything in the registry
        if data_stream == VOICE_RECORDING and chunk_hash.strip() in registry_hashes:
            continue
        
        # clear "CHUNKED_DATA/" from path, pull from the registry, compare hashes if they exist...
        # The hashes can have new lines at the end.
        path = chunkdata['chunk_path'].replace("CHUNKED_DATA/", "", 1)
        registry_hash = registry_dict.get(path)
        if registry_hash is not None and chunk_hash.strip() == registry_hash.strip():
            continue
        
        yield chunkdata


def combined_chunk_query(chunkregistry_query: QuerySet, values_params: list[str]) -> QuerySet:
    """ The hash value in the ChunkRegistry table is bad, we want to use the one from the S3File
    table. That query is ... problemy in django, best option so far is to use a subquery. This
    function takes the chunkregistry query and adds the sha1 field. """
    
    # the path value in the subquery has to be an exact match, not a startswith, or else postgres
    # will walk the whole database table / just not use the index correctly.
    # (append ".zst" to the chunk path to get the S3File path)
    s3file_zst_path = Concat(OuterRef("chunk_path"), Value(".zst"), output_field=(CharField()))
    
    # there is only one possible match (unique) but we have to limit it.
    sha1_subquery = S3File.objects.filter(path=s3file_zst_path).values_list("sha1")[:1]
    # size_subquery = S3File.objects.filter(path=s3file_zst_path).values_list("size_compressed")[:1]
    
    # wrap the subquery, retrieve the chunk and the sha1
    return chunkregistry_query.annotate(
            sha1=Subquery(sha1_subquery),
            # size_compressed=Subquery(size_subquery),
        ).values(*values_params, "sha1")  # "sha1", "size_compressed")


# The below is attempts to make the annoying registry query use a join in the database

# def combined_chunk_query2(**chunk_filters):
#     s3file_zst_path = Concat(OuterRef("chunk_path"), Value(".zst"), output_field=(CharField()))
#     s3 = S3File.objects.filter(path=s3file_zst_path)
#     from django.db import connection
#     meta_str_of_filter, modulo_params = ChunkRegistry.objects.filter(**chunk_filters).query.as_sql(SQLCompiler, connection)
#     str_of_whole_filter = meta_str_of_filter % modulo_params
#     just_the_where_clause = "WHERE" +str_of_whole_filter.split("WHERE", 1)[1]

#     a_query = f"""
#     SELECT
#     c.chunk_path,
#     s.sha1,
#     s.size_compressed
#     FROM database_chunkregistry c
#     LEFT JOIN database_s3file s
#     ON s.path = (COALESCE(c.chunk_path, '') || '.zst')
#     {just_the_where_clause}
#     """
#     print(a_query)
#     with connection.cursor() as cursor:
#         cursor.execute(a_query)
#         raw = cursor.fetchall()
#    return raw


# def combined_chunk_query(time_bin_gte: datetime, time_bin_lte: datetime):
#     from django.db import connection
#     from django.db.models import CharField

#     s3file_zst_path = Concat(OuterRef("chunk_path"), Value(".zst"), output_field=(CharField()))

#     # reference: "WHERE c.time_bin > 2025-11-30 19:49:43.446083+00:00"

#     time_filter_gte = f" c.time_bin >= '{time_bin_gte.isoformat()}' "
#     time_filter_lte = f" c.time_bin <= '{time_bin_lte.isoformat()}' "

#     a_query = """
#     SELECT
#     c.chunk_path,
#     s.sha1,
#     s.size_compressed
#     FROM database_chunkregistry c
#     """

#     previous = False
#     if time_bin_lte or time_bin_gte:
#         a_query += " WHERE "

#     if time_bin_gte:
#         previous = True
#         a_query += time_filter_gte

#     if time_bin_lte:
#         if previous:
#             a_query += " AND "
#         previous = True
#         a_query += time_filter_lte

#     a_query += f"""
#     LEFT JOIN database_s3file s
#     ON s.path = (COALESCE(c.chunk_path, '') || '.zst');
#     """

#     print(a_query)
#     with connection.cursor() as cursor:
#         cursor.execute(a_query)
#         raw = cursor.fetchall()
#     return raw
