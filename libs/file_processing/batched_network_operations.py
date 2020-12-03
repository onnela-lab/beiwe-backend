import codecs
import sys
import traceback
from datetime import datetime
from typing import Tuple

from database.data_access_models import ChunkRegistry
from database.survey_models import Survey
from database.user_models import Participant


GLOBAL_TIMESTAMP = datetime.now().isoformat()


def batch_upload(upload: Tuple[ChunkRegistry, str, bytes, str]):
    """ Used for mapping an s3_upload function.  the tuple is unpacked, can only have one parameter. """
    ret = {'exception': None, 'traceback': None}
    try:
        if len(upload) != 4:
            # upload should have length 4; this is for debugging if it doesn't
            print("upload length not equal to 4: ", upload)

        chunk, chunk_path, new_contents, study_object_id = upload
        del upload

        if "b'" in chunk_path:
            raise Exception(chunk_path)

        with open("processing_tests/" + GLOBAL_TIMESTAMP, 'ba') as f:
            f.write(b"\n\n")
            f.write(codecs.decode(new_contents, "zip"))

        # s3_upload(chunk_path, codecs.decode(new_contents, "zip"), study_object_id, raw_path=True)

        if isinstance(chunk, ChunkRegistry):
            # If the contents are being appended to an existing ChunkRegistry object
            chunk.file_size = len(new_contents)
            chunk.update_chunk_hash(new_contents)

        else:
            # If a new ChunkRegistry object is being created
            # Convert the ID's used in the S3 file names into primary keys for making ChunkRegistry FKs
            participant_pk, study_pk = Participant.objects.filter(patient_id=chunk['user_id']).values_list('pk', 'study_id').get()
            if chunk['survey_id']:
                survey_pk = Survey.objects.filter(object_id=chunk['survey_id']).values_list('pk', flat=True).get()
            else:
                survey_pk = None

            ChunkRegistry.register_chunked_data(
                chunk['data_type'],
                chunk['time_bin'],
                chunk['chunk_path'],
                new_contents,  # unlikely to be huge
                study_pk,
                participant_pk,
                survey_pk,
            )

    # it broke. print stacktrace for debugging
    except Exception as e:
        traceback.print_exc()
        ret['traceback'] = sys.exc_info()
        ret['exception'] = e

    return ret