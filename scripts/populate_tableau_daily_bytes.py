from collections import defaultdict
from datetime import date, datetime
from multiprocessing.pool import ThreadPool
from pprint import pprint
from typing import Any

from django.db.models import Min

from constants.data_stream_constants import CHUNKABLE_FILES
from database.models import ChunkRegistry, Participant, SummaryStatisticDaily
from database.study_models import Study
from libs.s3 import s3_get_size
from libs.utils.date_utils import get_timezone_shortcode


#
# Once compression is enabled this will be an invalid script
# The script will have to be rewritten to look up data in the new S3files table
#
# This script checks the chunk registry for files, gets their size from s3,
# then updates the chunks and the daily summary statistics. with that size.
#


def mapped_size_getter(raw_chunk_path):
    # we need the chunk theat we pass in back for the insertion
    # print(raw_chunk_path)
    return raw_chunk_path, s3_get_size(raw_chunk_path)


def calculate_data_quantity_stats(participant: Participant):
    """ Update the SummaryStatisticDaily  stats for a participant, using ChunkRegistry data """
    day: date
    time_bin: datetime
    
    daily_data_quantities: dict[Any, dict[Any, Any]] = defaultdict(lambda: defaultdict(int))
    days: set[date] = set()
    study_timezone = participant.study.timezone
    
    query = ChunkRegistry.objects.filter(
        participant=participant,
        data_type__in=CHUNKABLE_FILES
    ).values_list("chunk_path", "pk", 'time_bin', 'data_type').iterator()
    
    query_by_raw_path = {items[0] : list(items) for items in query}
    del query
    
    print(f"threadpool operations start")
    with ThreadPool(10) as pool:
        pool_out = pool.map(mapped_size_getter, query_by_raw_path.keys())
    print(f"threadpool operations end")
    
    print("setting up updates")
    for raw_chunk_path, size in pool_out:
        # size is oversize by 16 bytes because of the iv
        query_by_raw_path[raw_chunk_path].append(size-16)
    
    chunk_updates = []
    # Construct a dict formatted like this: dict[date][data_type] = total_bytes
    for _chunk_path, pk, time_bin, data_type, file_size in query_by_raw_path.values():
        day = time_bin.astimezone(study_timezone).date()
        days.add(day)
        daily_data_quantities[day][data_type] += file_size or 0
        chunk_updates.append(ChunkRegistry(pk=pk, file_size=file_size))
    
    print(f"updating {len(days)} daily summaries.")
    # print(f"updating {len(days)} daily summaries: {', '.join(day.isoformat() for day in sorted(days))}")
    
    # For each date, create a dict for SummaryStatisticDaily update_or_create asd pass it through
    for day, day_data in daily_data_quantities.items():
        data_quantity = {
            "participant": participant,
            "date": day,
            "defaults": {"timezone": get_timezone_shortcode(day, study_timezone)},
        }
        for data_type, total_bytes in day_data.items():
            data_quantity["defaults"][f"beiwe_{data_type}_bytes"] = total_bytes
        
        try:
            SummaryStatisticDaily.objects.update_or_create(**data_quantity)
        except Exception:
            # if something fails we need the data_quantity dict contents displayed in a log
            pprint(data_quantity)
            raise
    
    print("updating chunks:", ChunkRegistry.objects.bulk_update(chunk_updates, ["file_size"]))


# for participant in Participant.objects.all():
for study in Study.fltr(object_id=EDIT THE SCRIPT AND INSERT YOUR STUDY ID HERE):
    for participant in study.participants.all():
        print(participant.patient_id, "...", end=" ")
        calculate_data_quantity_stats(participant)


# go through all studies and ensure that data in the daily summary statistics table is accessible in the forest task dispatch.
for study in Study.objects.all():
    print(f"checking '{study.name}' for data earlier than study creation")
    
    q = SummaryStatisticDaily.objects.filter(participant__study_id=study.pk).aggregate(min=Min("date"))
    earliest = q['min']
    if not earliest:
        print("no data\n")
        continue
    
    print("earliest:", earliest)
    print("created_on:", study.created_on.date())
    
    if earliest < study.created_on.date():
        print(f"there is data that predates the creation date, created: {study.created_on.date()}")
        study.created_on = datetime.combine(earliest, study.created_on.time(), study.created_on.tzinfo)
        print("set to:", study.created_on)
        study.save()
    print()