import csv
from datetime import datetime, timedelta
from io import StringIO

import bleach
import orjson
from dateutil.tz import UTC
from django.db.models.fields import Field
from django.db.models.functions import Substr
from django.http import FileResponse, HttpRequest, StreamingHttpResponse
from django.http.response import HttpResponse
from django.shortcuts import render
from django.utils import timezone
from django.views.decorators.http import require_GET, require_POST

from authentication.data_access_authentication import (api_credential_check,
    api_study_credential_check, ApiResearcherRequest, ApiStudyResearcherRequest)
from authentication.tableau_authentication import authenticate_tableau, TableauRequest
from config.jinja2 import easy_url
from constants.forest_constants import FIELD_TYPE_MAP, SERIALIZABLE_FIELD_NAMES
from constants.message_strings import MISSING_JSON_CSV_MESSAGE
from constants.user_constants import TABLEAU_TABLE_FIELD_TYPES
from database.models import DataProcessingStatus, Study, StudyRelation, SummaryStatisticDaily
from libs.efficient_paginator import EfficientQueryPaginator
from libs.endpoint_helpers.copy_study_helpers import study_settings_fileresponse
from libs.endpoint_helpers.data_api_helpers import (check_request_for_omit_keys_param,
    DeviceStatusHistoryPaginator, get_validate_participant_from_request,
    participant_archived_event_dict)
from libs.endpoint_helpers.participant_table_helpers import (common_data_extraction_for_apis,
    get_table_columns)
from libs.endpoint_helpers.study_summaries_helpers import get_participant_data_upload_summary
from libs.endpoint_helpers.summary_statistic_helpers import summary_statistics_request_handler
from libs.intervention_utils import intervention_survey_data, survey_history_export


#
## Weird system status endpoint
#

def background_processing_status(request: HttpRequest):
    # the timer gets updated every 6 minutes, so a time substantially between two runs is best.
    last_run = DataProcessingStatus.singleton().last_run
    status = 500 if last_run is None or last_run < timezone.now() - timedelta(minutes=20) else 200
    return HttpResponse("", status=status)


## Study Data API Endpoints


@require_POST
@api_credential_check
def get_studies(request: ApiResearcherRequest):
    """
    Retrieve a dict containing the object ID and name of all Study objects that the user can access
    If a GET request, access_key and secret_key must be provided in the URL as GET params. If
    a POST request (strongly preferred!), access_key and secret_key must be in the POST
    request body.
    """
    # site admin needs to get all non-deleted studies
    if request.api_researcher.site_admin:
        return HttpResponse(
            orjson.dumps(
                dict(Study.objects.filter(deleted=False).values_list("object_id", "name"))
            )
        )
    
    # non-site-admins get their related studies
    return HttpResponse(
        orjson.dumps(
            dict(
                StudyRelation.objects.filter(researcher=request.api_researcher, study__deleted=False)
                .values_list("study__object_id", "study__name")
            )
        )
    )


@require_POST
@api_study_credential_check()
def get_participant_ids_in_study(request: ApiStudyResearcherRequest):
    """ Returns a JSON response of the participant IDs in the study."""
    # json can't operate on queryset, need as list.
    participants = list(request.api_study.participants.values_list('patient_id', flat=True))
    return HttpResponse(orjson.dumps(participants), status=200, content_type="application/json")


@require_POST
@api_study_credential_check()
def download_study_interventions(request: ApiStudyResearcherRequest):
    """ Returns a JSON response of the intervention data for a study. """
    data = intervention_survey_data(request.api_study)
    return HttpResponse(orjson.dumps(data), status=200, content_type="application/json")


@require_POST
@api_study_credential_check()
def download_study_survey_history(request: ApiStudyResearcherRequest):
    """ Returns a JSON response of the history of survey edits for a study."""
    study = request.api_study
    fr = FileResponse(
        survey_history_export(study).decode(),  # okay, whatever, it needs to be a string, not bytes
        content_type="text/json",
        as_attachment=True,
        filename=f"{study.object_id}_surveys_history_data.json",
    )
    fr.set_headers(None)  # django is still stupid?
    return fr


@require_POST
@api_study_credential_check()
def get_study_settings_file(request: ApiStudyResearcherRequest):
    """ Returns a JSON response of the study settings for a study."""
    # this endpoint already has tests via the study_settings_fileresponse function
    # but the tests are very minimal.  This code is quite complex....
    return study_settings_fileresponse(request.api_study.pk)


@require_POST
@api_study_credential_check()
def get_participant_data_quantities(request: ApiStudyResearcherRequest, study_id: str = None):
    """ Returns a JSON response of the participant data upload summaries. """
    summary_data = get_participant_data_upload_summary(request.api_study)
    return HttpResponse(orjson.dumps(summary_data), status=200, content_type="application/json")


@require_POST
@api_study_credential_check()
def get_participant_table_data(request: ApiStudyResearcherRequest):
    """ Returns a streaming JSON response of the participant data for Tableau."""
    data_format = request.POST.get("data_format", None)
    
    # error with message for bad data_format
    if data_format not in ("json", "json_table", "csv"):
        return HttpResponse(MISSING_JSON_CSV_MESSAGE, status=400)
    
    column_names = get_table_columns(request.api_study, frontend=False)
    table_data = common_data_extraction_for_apis(request.api_study, for_json=data_format != "csv")
    
    # virtually if not literally identical to the button api
    if data_format == "csv":
        buffer = StringIO()
        writer = csv.writer(buffer, dialect="excel")
        writer.writerow(column_names)  # write the header row
        writer.writerows(table_data)
        buffer.seek(0)
        return HttpResponse(buffer.read(), content_type='text/csv')
    
    if data_format == "json":
        return HttpResponse(
                orjson.dumps([dict(zip(column_names, row)) for row in table_data]),
                content_type="application/json",
            )
    
    if data_format == "json_table":
        # just return the table data as a json list of lists, but insert a first row of table names.
        table_data.insert(0, column_names)
        return HttpResponse(orjson.dumps(table_data), status=200, content_type="application/json")
    
    assert False, "unreachable code."


@require_GET
@authenticate_tableau
def get_tableau_participant_table_data(request: TableauRequest, study_object_id: str):
    """ Returns a streaming JSON response of the participant data for Tableau."""
    # the study object is already validated to exist and to be accessible to the requester
    study = Study.objects.get(object_id=study_object_id)
    table_data = common_data_extraction_for_apis(study, for_json=True)
    # Tableau requires keys to be alphanumerics and _
    column_names = [n.lower().replace(" ", "_") for n in get_table_columns(study, frontend=False)]
    
    ## Tableau and datetimes (and timezones)
    # Tableau does not like timezone-aware datetimes despite osjson being incredibly pedantic
    # about how it formats them to align with Javascript expectations.  These incoming datetimes
    # are in the study timezone, we remove the timezone and LEAVE IT NUMERICALLY INTACT.
    # Tableau (desktop) consumes the datetime and *does nothing* with it whether it has timezone
    # information or not.  Tableau explicitly does not support timezone-aware datetimes, or at least
    # we could not find a format it supported _generally_. IT DOES support 00:00Z style timezone,\
    # or at least it does puke like it does with 00:00+00:00 style timezonses, but doesn't do
    # anything with that information (as far as we can tell).  There is an app-global setting to
    # set your timezone, but we don't want to interact with that because we want these to _be in
    # the study timezone_ -- So, we remove the timezone information and leave it as numeric.
    for i, row in enumerate(table_data):
        table_data[i] = [
            (x if not isinstance(x, datetime) else x.replace(tzinfo=None)) for x in row
        ]
    
    datums = [dict(zip(column_names, row)) for row in table_data]
    
    # also we absolutely do not need any sub-minute precision so we will drop at least microseconds
    return HttpResponse(
        orjson.dumps(datums, option=orjson.OPT_OMIT_MICROSECONDS),
        content_type="application/json",
    )

## Summary Statistics and Forest Output


@require_POST
@api_study_credential_check()
def get_summary_statistics(request: ApiStudyResearcherRequest, study_id: str = None):
    """ Endpoint for summary statistics and forest output using data api credentialing details. """
    return summary_statistics_request_handler(request, request.api_study.object_id)


@require_GET
@authenticate_tableau
def get_tableau_summary_statistics(request: TableauRequest, study_object_id: str = None):
    """ Endpoint for summary statistics and forest output using tableau api credentialing details. """
    return summary_statistics_request_handler(request, study_object_id)


# todo: just make a wdc for the participant table data
# because tableau creates external dependencies of real code hitting an endpoint, so we can neeeeever change this.... sorta


@require_GET
def web_data_connector_summary_statistics(request: TableauRequest, study_object_id: str):
    """ Build the "columns" data structure for tableau to enumerate the format of the API data.
    This is an open endpoint, it does not require any authentication. """
    # study_id and participant_id are not part of the SummaryStatisticDaily model, so they aren't
    # populated. They are also related fields that both are proxies for a unique identifier field
    # that has a different name, so we do it manually.
    columns = [
        '[\n',
        "{id: 'study_id', dataType: tableau.dataTypeEnum.string,},\n",
        "{id: 'participant_id', dataType: tableau.dataTypeEnum.string,},\n"
    ]
    
    final_serializable_fields: list[Field] = [
        f for f in SummaryStatisticDaily._meta.fields if f.name in SERIALIZABLE_FIELD_NAMES
    ]
    
    for field in final_serializable_fields:
        for (python_type, tableau_type) in FIELD_TYPE_MAP:
            if isinstance(field, python_type):
                columns.append(f"{{id: '{field.name}', dataType: {tableau_type},}},\n")
                # example line: {id: 'participant_id', dataType: tableau.dataTypeEnum.int,},
                break
        else:
            # if the field is not recognized, supply it to tableau as a string type
            columns.append(f"{{id: '{field.name}', dataType: tableau.dataTypeEnum.string,}},\n")
    columns = "".join(columns) + '];'
    
    target_url = easy_url('data_api_endpoints.get_tableau_summary_statistics', study_object_id=study_object_id)
    return render(
        request,
        'wdc.html',
        context=dict(
            study_object_id=study_object_id,
            cols=columns,
            target_url=target_url,
        )
    )


@require_GET
def web_data_connector_participant_table(request: TableauRequest, study_object_id: str):
    """ Build the "columns" data structure for tableau to enumerate the format of the API data.
    This is an open endpoint, it does not require any authentication. """
    
    # this is an open endpoint, it requires a study object id and has non-static data that is specific
    # to the study. We have to check for this study object id in the database safely.
    study_object_id = bleach.clean(study_object_id)
    if len(study_object_id) != 24 or not study_object_id.isalnum():
        return HttpResponse("Invalid study id", status=400)
    if not Study.objects.filter(object_id=study_object_id).exists():
        return HttpResponse("Study not found", status=404)
    
    study = Study.objects.get(object_id=study_object_id)
    columns = ['[\n']
    column_names = [column.lower().replace(" ", "_") for column in get_table_columns(study, False)]
    
    # if the field isn't recognized, custom field and intervention names, use tableau.dataTypeEnum.string
    for name in column_names:
        if name not in TABLEAU_TABLE_FIELD_TYPES:
            t_type = "tableau.dataTypeEnum.string"
        else:
            t_type = TABLEAU_TABLE_FIELD_TYPES[name]
        columns.append(f"{{id: '{name}', dataType: {t_type},}},\n")
    columns = "".join(columns) + '];'
    
    target_url = easy_url(
        'data_api_endpoints.get_tableau_participant_table_data', study_object_id=study_object_id
    )
    return render(
        request,
        'wdc.html',
        context=dict(study_object_id=study_object_id, cols=columns, target_url=target_url)
    )


## New api endpoints for participant metadata


@require_POST
@api_credential_check
def get_participant_upload_history(request: ApiStudyResearcherRequest):
    """ Returns a streaming JSON response of the upload history of a participant."""
    
    participant = get_validate_participant_from_request(request)
    omit_keys = check_request_for_omit_keys_param(request)
    
    # EXTREMELY OBSCURE DETAIL: the annotated pseudofield "field_name" is forced to come after real
    # fields in the key ordering on query.values _but not on query.values_list_???
    FIELDS_TO_SERIALIZE = ["file_size", "timestamp", "file_name"]
    
    # We want to reduce the amount of raw data, so we strip out some unnecessary details both in
    # the query and using orjson options.
    # the file path string contains the patient id, let's remove it and the slash afterwords.
    # Substr(expression, pos, length=None, **extra) - pos is 1-indexed, length  of none means to the end.
    
    query = participant.upload_trackers.order_by("timestamp")
    start = len(participant.patient_id) + 2
    query = query.annotate(file_name=Substr("file_path", start, length=None))
    
    # we use our efficient paginator class to stream the bytes of the database query.
    paginator = EfficientQueryPaginator(
        filtered_query=query,
        page_size=10000,
        values=FIELDS_TO_SERIALIZE if not omit_keys else None,
        values_list=FIELDS_TO_SERIALIZE if omit_keys else None,
    )
    
    # OPT_OMIT_MICROSECONDS - obvious
    # OPT_UTC_Z - UTC timezone serialized to Z instead of +00:00
    options = orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_UTC_Z
    return StreamingHttpResponse(
        paginator.stream_orjson_paginate(option=options), status=200, content_type="application/json"
    )


@require_POST
@api_credential_check
def get_participant_heartbeat_history(request: ApiStudyResearcherRequest):
    """ Returns a streaming JSON response of the heartbeat history of a participant. """
    
    participant = get_validate_participant_from_request(request)
    omit_keys = check_request_for_omit_keys_param(request)
    FIELDS_TO_SERIALIZE = ["timestamp"]
    
    # We want to reduce the amount of raw data, so we strip out some unnecessary details both in
    # the query and using orjson options.
    
    query = participant.heartbeats.order_by("timestamp")
    paginator = EfficientQueryPaginator(
        filtered_query=query,
        page_size=10000,
        values=FIELDS_TO_SERIALIZE if not omit_keys else None,
        values_list=FIELDS_TO_SERIALIZE if omit_keys else None,
    )
    # OPT_OMIT_MICROSECONDS - obvious
    # OPT_UTC_Z - UTC timezone serialized to Z instead of +00:00
    options = orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_UTC_Z
    return StreamingHttpResponse(
        paginator.stream_orjson_paginate(option=options), status=200, content_type="application/json"
    )


@require_POST
@api_credential_check
def get_participant_version_history(request: ApiStudyResearcherRequest):
    """ Returns a streaming JSON response of the app and os version history of a participant. """
    participant = get_validate_participant_from_request(request)
    omit_keys = check_request_for_omit_keys_param(request)
    FIELDS_TO_SERIALIZE = ["app_version_code", "app_version_name", "os_version", "created_on"]
    
    query = participant.app_version_history.order_by("created_on")
    paginator = EfficientQueryPaginator(
        filtered_query=query,
        page_size=10000,
        values=FIELDS_TO_SERIALIZE if not omit_keys else None,
        values_list=FIELDS_TO_SERIALIZE if omit_keys else None,
    )
    # OPT_OMIT_MICROSECONDS - obvious
    # OPT_UTC_Z - UTC timezone serialized to Z instead of +00:00
    options = orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_UTC_Z
    return StreamingHttpResponse(
        paginator.stream_orjson_paginate(option=options), status=200, content_type="application/json"
    )


@require_POST
@api_credential_check
def get_participant_device_status_report_history(request: ApiStudyResearcherRequest):
    """ Returns a streaming JSON response of the device status report history of a participant.
    This endpoint is only for debugging and development purposes, it requires a participant
    have the enable_extensive_device_info_tracking experiment enabled. """
    participant = get_validate_participant_from_request(request)
    
    if not participant.device_status_reports.exists():
        return HttpResponse("[]", content_type="application/json")
    
    # we rewrite compressed report to device_status
    FIELDS_TO_SERIALIZE = [
        "created_on", "endpoint", "app_os", "os_version", "app_version", "compressed_report"
    ]
    
    query = participant.device_status_reports.order_by("created_on")
    paginator = DeviceStatusHistoryPaginator(
        filtered_query=query,
        page_size=1000,
        values=FIELDS_TO_SERIALIZE,
    )
    
    # OPT_OMIT_MICROSECONDS - obvious
    # OPT_UTC_Z - UTC timezone serialized to Z instead of +00:00
    options = orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_UTC_Z
    return StreamingHttpResponse(
        paginator.stream_orjson_paginate(option=options), status=200, content_type="application/json"
    )


@require_POST
@api_credential_check
def get_participant_notification_history(request: ApiStudyResearcherRequest):
    """ Returns a JSON response of the notification history of a participant. """
    participant = get_validate_participant_from_request(request)
    
    # determine study or utc for the display timezone
    tz = UTC if "utc" in [k.lower() for k in request.POST] else participant.study.timezone
    resp_dict = participant_archived_event_dict(participant, tz)
    
    # OPT_OMIT_MICROSECONDS - obvious
    # OPT_UTC_Z - UTC timezone serialized to Z instead of +00:00
    options = orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_UTC_Z
    resp_bytes = orjson.dumps(resp_dict, option=options)
    return HttpResponse(resp_bytes, status=200, content_type="application/json")
