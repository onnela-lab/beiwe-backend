from __future__ import annotations

import json
from typing import List, Union

from django.db.models import F
from django.http import HttpResponse, StreamingHttpResponse
from orjson import dumps as orjson_dumps

from constants.tableau_api_constants import SERIALIZABLE_FIELD_NAMES
from database.forest_models import SummaryStatisticDaily
from forms.django_forms import ApiQueryForm
from libs.internal_types import ResearcherRequest, TableauRequest
from libs.utils.effiicient_paginator import EfficientQueryPaginator


def summary_statistics_request_handler(
    request: Union[TableauRequest, ResearcherRequest], study_object_id: str
):
    form = ApiQueryForm(data=request.GET)
    if not form.is_valid():
        return HttpResponse(
            format_summary_data_errors(form.errors.get_json_data()), status=400, content_type="application/json"
        )
    
    # The don't need to specify the study_id and participant_id fields, those are provided.
    query_fields = [f for f in form.cleaned_data["fields"] if f in SERIALIZABLE_FIELD_NAMES]
    paginator = summary_statistics_api_query_database(
        study_object_id=study_object_id,  # the object id is validated in the login logic
        query_fields=query_fields,
        **form.cleaned_data,  # already cleaned and validated
    )
    return StreamingHttpResponse(paginator.stream_orjson_paginate(), content_type="application/json")


def format_summary_data_errors(errors: dict) -> str:
    """ Flattens a django validation error dictionary into a json string. """
    messages = []
    for field, field_errs in errors.items():
        messages.extend([err["message"] for err in field_errs])
    return json.dumps({"errors": messages})


def summary_statistics_api_query_database(
    study_object_id, participant_ids=None, limit=None,  # basics
    end_date=None, start_date=None,                     # time
    ordered_by="date", order_direction="default",       # sort
    query_fields: List[str] = None,
    **_  # Because Whimsy is important.                 # ignore everything else
) -> SummaryStatisticsPaginator:
    """ Args:
        study_object_id (str): study in which to find data
        start_date (optional[date]): first date to include in search
        end_date (optional[date]): last date to include in search
        limit (optional[int]): maximum number of data points to return
        order_by (str): parameter to sort output by. Must be one in the list of fields to return
        order_direction (str): order to sort in, either "ascending" or "descending"
        participant_ids (optional[list[str]]): a list of participants to limit the search to
    Returns EfficientPaginator of the SummaryStatisticsDaily objects specified by the parameters """
    
    if not query_fields:
        raise Exception("invalid usage")
    
    # Set up filter, order_by, and annotation quargs
    filter_kwargs = {}
    filter_kwargs["participant__study__object_id"] = study_object_id
    if participant_ids:
        filter_kwargs["participant__patient_id__in"] = participant_ids
    if end_date:
        filter_kwargs["date__lte"] = end_date
    if start_date:
        filter_kwargs["date__gte"] = start_date
    
    # participant_id needs to be remapped to patient_id
    if ordered_by == "participant_id":
        ordered_by = "patient_id"  # participant__patient_id also works, participant_id does not
    
    # default ordering for date (which is itself the default oreding) is most recent first
    if order_direction == "default" and ordered_by == "date":
        order_direction = "descending"
    elif order_direction == "default":
        order_direction = "ascending"
    if order_direction == "descending":
        ordered_by = "-" + ordered_by
    
    # Set up annotation to rename the study's object_id the "study_id"
    annotate_kwargs = {"study_id": F("participant__study__object_id")}
    if "participant_id" in query_fields:
        # need to replace participant_id with patient_id, we have to swap it back later
        query_fields[query_fields.index("participant_id")] = "patient_id"
        annotate_kwargs["patient_id"] = F("participant__patient_id")
    
    # construct query, apply limit if any, pass to paginator with large page size and return.
    query = SummaryStatisticDaily.objects \
        .annotate(**annotate_kwargs).order_by(ordered_by).filter(**filter_kwargs)
    
    return SummaryStatisticsPaginator(query, 10000, values=query_fields, limit=limit)


class SummaryStatisticsPaginator(EfficientQueryPaginator):
    # Handles a small data mutation, we need to convert patient_id back to participant_id, because
    # that is literally already a foreign key field so we couldn't include it under that name.
    # Note that this class is used in the csv export button too.
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # we can save some very minor time later by only having a mutation function if we actually
        # need it
        if  "patient_id" in self.field_names:
            self.mutate_query_results = self._mutate_query_results
        
    def _mutate_query_results(self, page: List[dict]):
        for values_dict in page:
            values_dict["participant_id"] = values_dict.pop("patient_id")
