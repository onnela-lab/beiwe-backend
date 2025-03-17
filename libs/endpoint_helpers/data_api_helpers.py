from typing import List

import orjson

from database.user_models_participant import Participant
from database.user_models_researcher import StudyRelation
from libs.efficient_paginator import EfficientQueryPaginator
from libs.internal_types import ApiStudyResearcherRequest
from libs.utils.compression import decompress
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
                row["device_status"] = orjson.Fragment(decompress(device_status))
