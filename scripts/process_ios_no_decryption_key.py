from datetime import timedelta

from django.utils import timezone

from endpoints.mobile_endpoints import upload_and_create_file_to_process_and_log
from database.data_access_models import IOSDecryptionKey
from database.system_models import GenericEvent
from database.user_models_participant import Participant
from libs.encryption import DeviceDataDecryptor
from libs.internal_types import GenericEventQuerySet
from libs.s3 import s3_retrieve


def generic_events_to_process() -> GenericEventQuerySet:
    """ Gets the GenericEvent objects that we need to process """
    # base query
    query = GenericEvent.objects.filter(tag="problem_upload_file_IosDecryptionKeyNotFoundError")
    # get the s3 file paths worked out
    s3_paths_to_event_pks = {
        note.split(" ")[0].split("/", 2)[-1][:-10]: pk
        for note, pk in list(query.values_list("note", "pk"))
        # if note.startswith("PROBLEM_UPLOADS/")
    }
    
    pks_to_get = [
        s3_paths_to_event_pks[path]
        for path in IOSDecryptionKey.objects
            .filter(file_name__in=s3_paths_to_event_pks.keys())
            .values_list("file_name", flat=True)
    ]
    
    return GenericEvent.objects.filter(pk__in=pks_to_get)


def decrypt_and_upload(generic_events_query: GenericEventQuerySet):
    """ Runs the uploaded problematic files through decryption.
    
    an example s3 file path looks like this (last ten chars are random-unique):
    PROBLEM_UPLOADS/study_object_id/patient_id/powerState/1652107854439.csvze8by89ez1 """
    print(f"found {generic_events_query.count()} ios upload files to add for processing")
    for event in generic_events_query:
        
        s3_path: str = event.note.split(" ")[0]  # the start of the note is the file path
        assert s3_path.startswith("PROBLEM_UPLOADS/")  # if this fails we are architecturally broken
        print(f"processing {s3_path}")
        # mimic the s3_file_location variable from mobile_api, get reequired pieces, get file.
        s3_file_location = s3_path.split("/", 2)[-1][:-10]
        patient_id = s3_file_location.split("/")[0]
        participant = Participant.objects.get(patient_id=patient_id)
        file_contents = s3_retrieve(s3_path, participant, raw_path=True)
        
        # don't wrap with any safety, if it fails, we crash right here.
        decryptor = DeviceDataDecryptor(s3_file_location, file_contents, participant)
        
        # handle upload logic from mobile_api
        upload_and_create_file_to_process_and_log(s3_file_location, participant, decryptor)
        event.delete()


decrypt_and_upload(generic_events_to_process())

# purge corrupted uploads that are over 30 days old from the generic events db
GenericEvent.objects.filter(tag="problem_upload_file_IosDecryptionKeyNotFoundError")\
    .filter(created_on__lt=timezone.now() - timedelta(days=30)).delete()