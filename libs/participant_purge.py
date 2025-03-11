import itertools
from typing import Generator

from django.utils import timezone

from constants import action_log_messages
from constants.common_constants import CHUNKS_FOLDER, PROBLEM_UPLOADS
from database.user_models_participant import Participant, ParticipantDeletionEvent
from libs.s3 import s3_delete_many_versioned, s3_list_files, s3_list_versions
from libs.utils.security_utils import generate_easy_alphanumeric_string


DELETION_PAGE_SIZE = 250


def add_participant_for_deletion(participant: Participant):
    """ Adds a participant to the deletion queue. """
    try:
        ParticipantDeletionEvent.objects.get(participant=participant)
        return
    except ParticipantDeletionEvent.DoesNotExist:
        pass
    ParticipantDeletionEvent.objects.create(participant=participant)


RELATED_NAMES = [
    "chunk_registries",
    "summarystatisticdaily_set",
    "lineencryptionerror_set",
    "iosdecryptionkey_set",
    "foresttask_set",
    "encryptionerrormetadata_set",
    "files_to_process",
    "pushnotificationdisabledevent_set",
    "fcm_tokens",
    "field_values",
    "upload_trackers",
    "intervention_dates",
    "scheduled_events",
    "archived_events",
    "heartbeats",
    "device_status_reports",
    "app_version_history",
    "notification_reports",
    "s3_files",
]


def run_next_queued_participant_data_deletion():
    """ checks ParticipantDeletionEvent for un-run events, runs deletion over all of them. """
    # only deletion events that have not been confirmed completed (purge_confirmed_time) and only
    # events that have last_updated times more than 30 minutes ago. (The deletion process constantly
    # updates the database with a count of files deleted as it runs.)
    deletion_event = ParticipantDeletionEvent.objects.filter(
        purge_confirmed_time__isnull=True,
        last_updated__lt=timezone.now() - timezone.timedelta(minutes=30),
    ).first()
    
    #! if you are writing a test and this is happening it might be because the last_updated field
    #! is specifically updated over in confirm_deleted as a control mechanism to prevent overlaps.
    if not deletion_event:
        return
    
    deletion_event.save()  # mark the event as processing...
    participant = deletion_event.participant
    
    # mark the participant as retired (field name is unregistered, its a legacy name), disable
    # easy enrollment, set a random password (validation runs on save so it needs to be valid)
    participant.log(action_log_messages.PARTICIPANT_DELETION_EVENT_STARTED)
    
    participant.update(
        permanently_retired=True, easy_enrollment=False, device_id="", os_type=""
    )
    participant.set_password(generate_easy_alphanumeric_string(50))
    
    delete_participant_data(deletion_event)
    # A meta test that checks that a test for every single related field is present will fail
    # whenever a new relation is added. You have to manually make that test.
    for name in RELATED_NAMES:
        getattr(participant, name).all().delete()
    
    #! BUT WE DON'T DELETE ACTION LOGS.
    # deletion_event.participant.action_logs.all().delete()
    confirm_deleted(deletion_event)
    participant.update(deleted=True)
    participant.log(action_log_messages.PARTICIPANT_DELETION_EVENT_DONE)


def delete_participant_data(deletion_event: ParticipantDeletionEvent):
    """ Deletes all files on S3 for a participant. """
    for page_of_files in all_participant_file_paths(deletion_event.participant):
        # The dev is Extremely Aware that s3_list_versions call Could just return the raw boto3-
        # formatted list of dicts, and that that is the form they are received in. We. Do. Not.
        # Care. Instead we choose the the path of valor: hating boto3 so much that we will repack
        # the data into a structure that makes sense and then unpack it. Overhead is negligible.
        s3_delete_many_versioned(page_of_files)
        # If it doesn't raise an error then all the files were deleted.
        deletion_event.files_deleted_count += len(page_of_files)
        deletion_event.save()  # ! updates the event's last_updated, indicating deletion is running.


def confirm_deleted(deletion_event: ParticipantDeletionEvent):
    """ Tests all locations for files and database entries, raises AssertionError if any are found. """
    deletion_event.save()  # mark the event as processing...
    keys, base, chunks_prefix, problem_uploads = get_all_file_path_prefixes(deletion_event.participant)
    for _ in s3_list_files(keys, as_generator=True):
        raise AssertionError(f"still files present in {keys}")
    for _ in s3_list_files(base, as_generator=True):
        raise AssertionError(f"still files present in {base}")
    for _ in s3_list_files(chunks_prefix, as_generator=True):
        raise AssertionError(f"still files present in {chunks_prefix}")
    for _ in s3_list_files(problem_uploads, as_generator=True):
        raise AssertionError(f"still files present in {problem_uploads}")
    
    for name in RELATED_NAMES:
        if getattr(deletion_event.participant, name).exists():
            raise AssertionError(f"still have database entries for {name}")
    
    #! BUT WE DON'T DELETE ACTION LOGS, in fact there should be at least 1
    if not deletion_event.participant.action_logs.exists():
        raise AssertionError("For some reason there are no action logs for this participant.")
    
    # mark the deletion event as _confirmed_ completed
    deletion_event.purge_confirmed_time = timezone.now()
    deletion_event.save()


def all_participant_file_paths(participant: Participant) -> Generator[list[tuple[str, str]], None, None]:
    """ Generator, iterates over over all files for a participant, yields pages of 100 file_paths
    and version ids at a time. """
    many_file_version_ids = []
    
    # there will inevitably be more than these sets of files, using chain for flexibility
    for s3_prefix in itertools.chain(get_all_file_path_prefixes(participant)):
        for key_path_version_id_tuple in s3_list_versions(s3_prefix):
            many_file_version_ids.append(key_path_version_id_tuple)
            # yield a page of files, reset page
            if len(many_file_version_ids) % DELETION_PAGE_SIZE == 0:
                yield many_file_version_ids
                print(many_file_version_ids)
                many_file_version_ids = []
    
    # yield any overflow files
    if many_file_version_ids:
        yield many_file_version_ids


# Note from developing the Forest task output file uploads - they are contained inside the regular
# participant data folder, the jasmine bv_dict etc. are derived and don't have ~pii. 👍 We good.
def get_all_file_path_prefixes(participant: Participant) -> tuple[str,str,str,str]:
    """ The singular canonical location of all locations where participant data may be stored. """
    base = participant.study.object_id + "/" + participant.patient_id + "/"
    chunks_prefix = CHUNKS_FOLDER + "/" + base
    problem_uploads = PROBLEM_UPLOADS + "/" + base
    # this one is two files at most without a trailing slash
    keys = participant.study.object_id + "/keys/" + participant.patient_id
    return keys, base, chunks_prefix, problem_uploads
