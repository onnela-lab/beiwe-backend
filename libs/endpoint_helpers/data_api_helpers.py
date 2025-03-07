from collections import defaultdict
from datetime import datetime, tzinfo
from typing import List

import orjson
import zstd

from constants.message_strings import MESSAGE_SEND_SUCCESS
from database.models import Participant
from database.user_models_participant import Participant, SurveyNotificationReport
from database.user_models_researcher import StudyRelation
from libs.efficient_paginator import EfficientQueryPaginator
from libs.internal_types import ApiStudyResearcherRequest
from middleware.abort_middleware import abort


def get_validate_participant_from_request(request: ApiStudyResearcherRequest) -> Participant:
    """ checks for a mandatory POST param participant_id, and returns the Participant object. 
    If participant_id is not present raise a 400 error. If the participant is not found a 404 error."""
    participant_id = request.POST.get('participant_id')
    if not participant_id:
        return abort(400)
    
    # raising a 404 on participant not found is not an information leak.
    # get_object_or_404 renders the 404 page, which is not what we want.
    try:
        participant = Participant.objects.get(patient_id=participant_id)
    except Participant.DoesNotExist:
        return abort(404)
    
    # authentication is weird because these endpoint doesn't have the mandatory study so code
    # patterns might change.
    # if the researcher is not a site admin, they must have a relationship to the study.
    if not request.api_researcher.site_admin:
        if not StudyRelation.determine_relationship_exists(
            study_pk=participant.study.pk, researcher_pk=request.api_researcher.pk
        ):
            return abort(403)
    
    return participant


def check_request_for_omit_keys_param(request):
    """ Returns true if the request has a POST param omit_keys set to case-insensitive "true". """
    omit_keys = request.POST.get("omit_keys", "false")
    return omit_keys.lower() == "true"


class DeviceStatusHistoryPaginator(EfficientQueryPaginator):
    def mutate_query_results(self, page: List[dict]):
        """ We need to decompress the json-encoded device status data field. """
        for row in page:
            device_status = row.pop("compressed_report")
            if device_status == b"empty":
                row["device_status"] = {}  # probably not reachable on real server
            else:
                # zstd compression is _very_ fast. A weak server processed 460,541 decompresses of
                # device infos in 1.179045 seconds in a tight loop.
                # orjson.Fragment is orjson's mechanism to pass ...subsegments? that are already
                # json encoded. This causes the output json to be an object, not a json string,
                # (And it's faster and avoids a bytes -> string -> bytes conversion.)
                row["device_status"] = orjson.Fragment(zstd.decompress(device_status))


def participant_archived_event_dict(p: Participant, tz: tzinfo) -> dict:
    """ Bulk code of the participant notification history endpoint. """
    created: datetime
    sched_time: datetime
    
    # sort by the survey, the time sent, then alphabetical schedule type name as tiebreaker
    query = p.archived_events.order_by(
        "survey_archive__survey__object_id",
        "created_on",
        "schedule_type",
    ).values_list(
        "survey_archive__survey__object_id",
        "created_on",
        "schedule_type",
        "scheduled_time",
        "status",
        "uuid",
        "was_resend",
    )
    
    p.notification_reports
    uuid_to_confirmed_time = SurveyNotificationReport.make_lookup_dict(
        {"participant": p}, "notification_uuid", "created_on"
    )
    
    jsonable_data = defaultdict(list)
    for survey_id, created, sched_type, sched_time, status, a_uuid, resend in query:
        # convert everything to the (study's timezone or UTC)
        sched_time = sched_time.astimezone(tz)
        created = created.astimezone(tz)
        
        jsonable_data[survey_id].append(
            {
                "timestamp": created,
                "type": sched_type,
                "scheduled_time": sched_time,
                "confirmed_received": uuid_to_confirmed_time.get(a_uuid, False),  
                "uuid": a_uuid,
                "resend": resend,
                "push_rejected": not (status == MESSAGE_SEND_SUCCESS), # could push into db....
            }
        )
    return dict(jsonable_data)
