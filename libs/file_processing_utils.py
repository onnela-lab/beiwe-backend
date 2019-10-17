from datetime import datetime
from multiprocessing.pool import ThreadPool

from config.constants import (
    CONCURRENT_NETWORK_OPS, CHUNKS_FOLDER, CHUNKABLE_FILES,
    PROCESSABLE_FILE_EXTENSIONS, data_stream_to_s3_file_name_string,
)
from libs.file_processing import process_file_chunks
from libs.s3 import s3_list_files, s3_delete, s3_upload
from database.data_access_models import ChunkRegistry, FileProcessLock, FileToProcess
from database.study_models import Study
from database.user_models import Participant


def reindex_all_files_to_process():
    """
    Totally clears the FilesToProcess DB, deletes all chunked files on S3,
    clears the ChunksRegistry DB, readds all relevant files on S3 to the
    FilesToProcess registry and then rechunks them.
    """
    raise Exception("This code has not been tested since converting database backends")
    # Delete all preexisting FTP and ChunkRegistry objects
    FileProcessLock.lock()
    print('{!s} purging FileToProcess: {:d}'.format(datetime.now(), FileToProcess.objects.count()))
    FileToProcess.objects.all().delete()
    print('{!s} purging ChunkRegistry: {:d}'.format(datetime.now(), ChunkRegistry.objects.count()))
    ChunkRegistry.objects.all().delete()
    
    pool = ThreadPool(CONCURRENT_NETWORK_OPS * 2)
    
    # Delete all preexisting chunked data files
    CHUNKED_DATA = s3_list_files(CHUNKS_FOLDER)
    print('{!s} deleting older chunked data: {:d}'.format(datetime.now(), len(CHUNKED_DATA)))
    pool.map(s3_delete, CHUNKED_DATA)
    del CHUNKED_DATA
    
    # Get a list of all S3 files to replace in the database
    print('{!s} pulling new files to process...'.format(datetime.now()))
    files_lists = pool.map(s3_list_files, Study.objects.values_list('object_id', flat=True))
    
    # For each such file, create an FTP object
    print("putting new files to process...")
    for i, l in enumerate(files_lists):
        print('{!s} {:d} of {:d}, {:d} files'.format(datetime.now(), i + 1, Study.objects.count(), len(l)))
        for fp in l:
            if fp[-4:] in PROCESSABLE_FILE_EXTENSIONS:
                patient_id = fp.split('/', 2)[1]
                participant_pk = Participant.objects.filter(patient_id=patient_id).values_list('pk', flat=True).get()
                FileToProcess.append_file_for_processing(fp, fp.split("/", 1)[0], participant_id=participant_pk)
    
    # Clean up by deleting large variables, closing the thread pool and unlocking the file process lock
    del files_lists, l
    pool.close()
    pool.terminate()
    FileProcessLock.unlock()
    
    # Rechunk the newly created FTPs
    print("{!s} processing data.".format(datetime.now()))
    process_file_chunks()


def reindex_specific_data_type(data_type):
    raise Exception("This code has not been tested since converting database backends")
    FileProcessLock.lock()
    print("starting...")
    # Convert the data type; raise an error if something is wrong with it
    file_name_key = data_stream_to_s3_file_name_string(data_type)

    # Get all chunk paths of the given data type
    relevant_chunks = ChunkRegistry.objects.filter(data_type=data_type)
    # list() ensures that the QuerySet is evaluated before all of its elements are deleted (otherwise it would be empty)
    relevant_indexed_files = list(relevant_chunks.values_list('chunk_path', flat=True))

    # Delete the old ChunkRegistry objects
    print("purging old data...")
    relevant_chunks.delete()

    pool = ThreadPool(20)
    pool.map(s3_delete, relevant_indexed_files)

    print("pulling files to process...")
    files_lists = pool.map(s3_list_files, Study.objects.values_list('object_id', flat=True))
    for i, l in enumerate(files_lists):
        print('{!s} {:d} of {:d}, {:d} files'.format(datetime.now(), i + 1, Study.objects.count(), len(l)))
        for fp in l:
            if fp[-4:] in PROCESSABLE_FILE_EXTENSIONS:
                patient_id = fp.split('/', 2)[1]
                participant_pk = Participant.objects.filter(patient_id=patient_id).values_list('pk', flat=True).get()
                FileToProcess.append_file_for_processing(fp, fp.split("/", 1)[0], participant_id=participant_pk)

    del files_lists, l
    pool.close()
    pool.terminate()
    FileProcessLock.unlock()

    print("{!s} processing data.".format(datetime.now()))
    process_file_chunks()
    print("Done.")


# def reindex_study(study_id):
#     if isinstance(study_id, (str, unicode)):
#         study_id = ObjectId(study_id)
#     study_id_string = str(study_id)
#     FileProcessLock.lock()
#     print "starting..."
#     #FIXME: this line is awful, it cause the data loss
#     raise Exception("ELI YOU ARE DUMB!")
#     # relevant_chunks = ChunksRegistry(study_id=study_id)
#     relevant_indexed_files = [ chunk["chunk_path"] for chunk in relevant_chunks ]
#     print "purging old data..."
#     for chunk in relevant_chunks: chunk.remove()
#
#     pool = ThreadPool(20)
#     pool.map(s3_delete, relevant_indexed_files)
#     pool.close( )
#     pool.terminate( )
#
#     print "pulling files to process..."
#     file_list = s3_list_files(study_id_string)
#     print "adding", len(file_list), "files to process"
#
#     for fp in file_list:
#         if fp[-4:] in PROCESSABLE_FILE_EXTENSIONS:
#             FileToProcess.append_file_for_processing(fp, study_id, fp.split("/", 2)[1])
#
#     del file_list, relevant_chunks, relevant_indexed_files, pool
#     print str(datetime.now()), "processing data..."
#     FileProcessLock.unlock()
#     process_file_chunks()
#     print "Done."


def check_for_bad_chunks():
    """ This function runs through all chunkable data and checks for invalid file pointers
    to s3. """
    chunked_file_paths = set(s3_list_files("CHUNKED_DATA"))
    bad_chunks = []
    for entry in ChunkRegistry.objects.all():
        if entry.data_type in CHUNKABLE_FILES and entry.chunk_path not in chunked_file_paths:
            bad_chunks.append(entry)
    print("bad chunks:", len(bad_chunks))

    # for chunk in bad_chunks:
    #     u = chunk.user_id
    #     print Study(_id=u.study_id).name


def count_study_chunks():
    chunked_file_paths = s3_list_files("CHUNKED_DATA")
    # The file paths start with CHUNKED_DATA/[24-digit object ID]
    study_prefixes = [f[:38] for f in chunked_file_paths]
    study_prefix_to_id = {study_prefix: study_prefix.split("/")[-2] for study_prefix in study_prefixes}
    study_prefix_to_name = {study_prefix: Study.objects.get(object_id=study_object_id).name for study_prefix, study_object_id in study_prefix_to_id.items()}
    print(study_prefix_to_name)
    study_count = {study_prefix_to_name[study_prefix]: len([f for f in chunked_file_paths if f[:38] == study_prefix]) for study_prefix in study_prefixes}
    return study_count


def create_fake_mp4(number=10):
    participant_id = Participant.objects.get(patient_id='h6fflp')
    for x in range(number):
        with open("thing", "r") as f:
            file_path = "55d3826297013e3a1c9b8c3e/h6fflp/voiceRecording/%s.mp4" % (1000000000 + x)
            s3_upload(file_path, f.read(), "55d3826297013e3a1c9b8c3e", raw_path=True)
            FileToProcess.append_file_for_processing(file_path, "55d3826297013e3a1c9b8c3e", participant_id=participant_id)


def completely_purge_study(study_id, actually_delete=False):
    study = Study.objects.get(object_id=study_id)

    surveys = study.surveys.all()
    device_settings = study.device_settings
    participants = study.participants.all()
    chunks = study.chunk_registries.all()
    files_to_process = study.files_to_process.all()
    if not actually_delete:
        print("if you actually delete this you will not be able to decrypt anything "
              "from this study.  Don't do it unless you know what you are doing.")
        print(study.name)
        # print len(study)
        # print len(device_settings)
        print(len(surveys))
        print(len(participants))
        print(len(chunks))
        print(len(files_to_process))
    else:
        device_settings.delete()
        surveys.delete()
        participants.delete()
        chunks.delete()
        files_to_process.delete()
        study.delete()
