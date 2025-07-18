from datetime import timedelta

from django.utils import timezone

from constants.common_constants import EARLIEST_POSSIBLE_DATA_DATE, EARLIEST_POSSIBLE_DATA_DATETIME
from constants.data_stream_constants import AMBIENT_AUDIO, AUDIO_RECORDING
from database.data_access_models import ChunkRegistry
from database.forest_models import SummaryStatisticDaily


# this has to be manually imported and run
def review_data():
    """ This is complex to put together, sticking it in an (unused) function."""
    query = ChunkRegistry.objects.filter(time_bin__lt=EARLIEST_POSSIBLE_DATA_DATETIME) \
        .exclude(data_type__in=[AMBIENT_AUDIO, AUDIO_RECORDING])
    
    bad_chunks = []
    print("\nSearching for clearly corrupted ChunkRegistries...\n")
    
    # we don't detect some all bad files, but there are a few easy patterns that should fix all
    # header mismatch exceptions due to unix-epoch start collisions.
    # (The real fix for this should be to detect the case at upload)
    print("\n\nThese files were found to include either corrupted or otherwise unusable data:")
    for chunk in query.order_by("time_bin"):
        header: bytes = chunk.s3_retrieve().splitlines()[0]
        
        # if the header starts with a comma that means the timestamp will be interpreted as 1970.
        # this is invariably junk (data without a timestamp is useless), delete it.
        if header.startswith(b","):
            print(f"{chunk.chunk_path}:")
            print(f"\tincomplete: {chunk.time_bin.isoformat()}: '{header.decode()}'")
            bad_chunks.append(chunk)
            continue
        
        # headers are english, and never have any extended unicode range characters. there are
        # several ways to do this, the least-obscure is to test for characters that are above 127 in
        # their ordinal (byte) value.
        for c in header:
            if c > 127:
                print(f"{chunk.chunk_path}:")
                print(f"\tcorrupted: {chunk.time_bin.isoformat()}: {header}")
                bad_chunks.append(chunk)
                continue
    
    if bad_chunks:
        # print("deleting bad chunks...")
        if input("delete stuff? y/n") == "y":
            print(ChunkRegistry.objects.filter(pk__in=[chunk.pk for chunk in bad_chunks]).delete())
    else:
        print("No obviously corrupted chunk registries were found.")
        exit(0)


# This will run from the command line tool
def main():
    # The longest timezone difference is 14 hours, our arbitrary cutoff will be 36 hours in the future.
    
    LATEST_POSSIBLE_DATA = timezone.now() + timedelta(hours=36)
    LATEST_POSSIBLE_DATA_DATE = LATEST_POSSIBLE_DATA.date()
    
    print("deleting any too-old chunks...")
    print(ChunkRegistry.objects.filter(time_bin__lt=EARLIEST_POSSIBLE_DATA_DATETIME).delete())
    print("deleting any too-old daily summaries...")
    print(SummaryStatisticDaily.objects.filter(date__lt=EARLIEST_POSSIBLE_DATA_DATE).delete())
    
    print("deleting any future chunks...")
    print(ChunkRegistry.objects.filter(time_bin__gt=LATEST_POSSIBLE_DATA).delete())
    print("deleting any future daily summaries...")
    # this is actually very generous at 2-3 days
    print(SummaryStatisticDaily.objects.filter(date__gt=LATEST_POSSIBLE_DATA_DATE).delete())
