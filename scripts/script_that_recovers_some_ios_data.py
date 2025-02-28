from datetime import datetime
from typing import Generator, List, Tuple

from Cryptodome.PublicKey import RSA
from dateutil.tz import gettz

from database.study_models import Study
from database.user_models_participant import Participant
from libs import encryption
from libs.encryption import (DecryptionKeyInvalidError, DefinitelyInvalidFile, DeviceDataDecryptor,
    InvalidData, InvalidIV, IosDecryptionKeyDuplicateError, IosDecryptionKeyNotFoundError,
    RemoteDeleteFileScenario, UnHandledError)
from libs.endpoint_helpers.participant_file_upload_helpers import (
    upload_and_create_file_to_process_and_log)
from libs.s3 import s3_list_files, s3_retrieve


# Please see the postmortem in this issue on Github for more information.
#    https://github.com/onnela-lab/beiwe-backend/issues/360 
# This script will not be intentionally maintained.


####################################################################################################
# This script will process all the files in the PROBLEM_UPLOADS folder of AWS S3, which contains all
# uploaded files that we were unable to decrypt. The existence of these files was due to a bug
# limited to iOS devices only. There was a race-condition that affected all .csv files and it could
# result in encryption keys failing to be present in uploaded files.
#
# 1) Sometimes the files were just junk, binary noise. I don't know when it was squashed, but it
#    was. These files are completely unparseable and are never expected to be recovered.
#
# 2) Sometimes the encryption key was not present - but two files with the same name, only one
#    containing the key, were uploaded. We added a capability to the backend to stash all decryption
#    keys in the database associated with those file names. We could then use them to decrypt files
#    lacking keys but matching the name. This required both an at-upload-time check and a periodic
#    script that checks the recently uploaded bad files and check for any keys. Code for this can be
#    found in /scripts/process_ios_no_decryption_key.py. The task for this script runs hourly.
#
# 3) Sometimes, and only observed as present in 2024 (after substantially rewriting iOS file-writing
#    code + thorough testing) the encryption key WAS present _but on the wrong line in the file_.
#    These files are fully recoverable, as are any instances of 2) that lost their keys to these
#    malformatted files.
#
####################################################################################################
#
# THIS SCRIPT...
# - Finds, decrypts, and sets up for processing all files affected by issue 3.
#
# - RE-processes all uploaded files that experienced issue 2.
#   - This is incredibly wasteful.
#   - This is because I can't work out how to determine if any given file has been processed at an
#     unknown time in the past.
#   - we could look at the created_on timestamp of the decryption key and make a heuristic guess???
#
# - Has been written with the intention of removing the architecture over in
#   /scripts/process_ios_no_decryption_key.py because We Have Fixed The Bug.
# 
# - Iterates over So Many Files that we can't even cache file names in memory.
#
# - At this point I'm considering making it a distributed celery task.
#
# - IS NOT FINISHED. I have been working on it on our staging server, its just so complex and gross.
#
# - Should probably delete the files in PROBLEM_UPLOADS after we are definitely completely done
#   processing them.
#
# - I don't know what the payoff actually is for this. It "seems like a lot" to me watching test
#   versions of the script execute on staging, but I would be astonished if it is 10%.
#   More data more better though, so its worth doing.
####################################################################################################


class NoKeyException(Exception): pass

t_start = datetime.now()

# enable logging to get verbose output of decryption processes.
encryption.ENABLE_DECRYPTION_LOG = False

UTC = gettz("UTC")
LOCAL_TIME_ZONE = gettz("America/New_York")  # just for display, no other effect


# these objects are either network or database calls to access, ~doubles our speed if we cache them.
STUDIES = {}
PARTICIPANTS = {}
RSA_KEYS = {}


# if you want to run this on only one participant or study you can filter here.
# example file path: PROBLEM_UPLOADS/5873fe38644ad7557b168e43/c3b7mk7j/gps/1664315342657.csv
# for example, to filter by study, change the S3_QUERY to "PROBLEM_UPLOADS/5873fe38644ad7557b168e43/"
# or for participant, change it to "PROBLEM_UPLOADS/5873fe38644ad7557b168e43/c3b7mk7j/"
S3_QUERY = "PROBLEM_UPLOADS/"

# I needed to exclude a bunch of participants. It just excludes file paths with these strings in them.
ANY_OTHER_EXCLUSION_FILTERS = [
    # "tnca5ih4","3brltc11","9zqq78ek","c1dn4w94","c3b7mk7j","f37twhxm","ksg8clpo","tlukstb3","cihq5v42","cihq5v42"
]

class STATS:
    DATA_READ_IN = 0
    DATA_PUSHED_OUT = 0
    FILES_EXAMINED = 0

# get all the file paths for a specific participant. S3 paths are returned in sortable order.
def get_some_problem_uploads_by_participant() -> Generator[List[str], None, None]:
    print("OK. Starting.")
    
    # setup, get first path
    ret_paths = []
    previous_patient_id = None
    
    # path is a string that looks like this, including those extra 10 characters at the end:
    #    PROBLEM_UPLOADS/5873fe38644ad7557b168e43/c3b7mk7j/gps/1664315342657.csvQWERTYUIOP
    for count, path in enumerate(s3_list_files(S3_QUERY, as_generator=True)):
        if count % 1000 == 0:
            print("File", count, "...")
        
        # we only want csv files (can't do endswith because of the random 10 character string at end of the file names.)
        if ".csv" not in path:
            continue
        
        # skip files that are filtered out
        skip_file = False
        for k in ANY_OTHER_EXCLUSION_FILTERS:
            if k in path:
                skip_file = True
                break  # spent 5 minutes with this as a continue.
        if skip_file:
            continue
        
        patient_id = path.split("/")[2]
        if patient_id == previous_patient_id:
            ret_paths.append(path)
        else:
            if ret_paths:
                yield ret_paths
            ret_paths = [path]
            previous_patient_id = patient_id
            print("\nStarting on participant", previous_patient_id, "...\n")
    
    yield ret_paths


# lines without a : at position 24 (the 25th character) may be decryption keys
# shift that line to the top of the list
def find_and_fix_candidate_key_line(lines: List[str]) -> List[str]:
    for i, line in enumerate(lines):
        if len(line) <= 24:
            continue
        if line[24] != ":":
            if len(line) == 344:  # it is always this length, that is how it works. I think.
                # we have a candidate key line
                lines.insert(0, lines.pop(i))
                return
    raise NoKeyException("No candidate key lines found")


def log(*args, **kwargs):
    # need a print function that doesn't end in a newline
    print(*args, **kwargs, end=" ", flush=True)


def process_a_participants_files(some_s3_paths: List[str]):
    for s3_file_location in some_s3_paths:
        STATS.FILES_EXAMINED += 1
        # because s3 files are sorted, duplicate files will be next to each other.
        process_a_participants_file(s3_file_location)
        print()  # all other print statements are on the same line, this is a separator.


def info(s3_file_location: str) -> Tuple[Study, Participant, RSA.RsaKey, str, str]:
    # file paths look like PROBLEM_UPLOADS/5873fe38644ad7557b168e43/c3b7mk7j/gps/1664315342657.csv
    
    # exactly the path string of the file when uploaded, which we need
    clean_file_path = "/".join(s3_file_location.split("/")[2:])[:-10]
    
    # second section between slashes is the study - stash it so we don't have to hit the database
    study_object_id = s3_file_location.split("/")[1]
    if study_object_id in STUDIES:
        study = STUDIES[study_object_id]
    else:
        study = Study.objects.get(object_id=study_object_id)
        STUDIES[study_object_id] = study
    
    # third is the participant id - stash it so we don't have to hit the database and s3
    patient_id = s3_file_location.split("/")[2]
    
    # get participant and rsa key from cache or database
    if patient_id in PARTICIPANTS:
        participant = PARTICIPANTS[patient_id]
        rsa_key = RSA_KEYS[patient_id]
    else:
        participant = Participant.objects.get(patient_id=patient_id)
        PARTICIPANTS[patient_id] = participant
        rsa_key = RSA_KEYS[patient_id] = participant.get_private_key()
    
    # we just need to print the timestamp, I needed it for development reasons to match my timezone.
    timestamp = int(s3_file_location.split("/")[-1].split(".")[0]) / 1000.0
    timestamp = datetime.fromtimestamp(timestamp).astimezone(UTC).astimezone(LOCAL_TIME_ZONE)
    timestamp = timestamp.strftime("%Y-%m-%d %I:%M:%S%p (%Z)")
    return study, participant, rsa_key, clean_file_path, timestamp


def process_a_participants_file(s3_file_location: str):
    # details...
    study, participant, rsa_key, clean_file_path, timestamp = info(s3_file_location)
    log(timestamp, clean_file_path)
    
    data: bytes = s3_retrieve(s3_file_location, study, raw_path=True)
    STATS.DATA_READ_IN += len(data)
    try:
        data = data.decode()
    except UnicodeDecodeError:
        log("hard no.")  # some files are completely garbled, they are dead to us.
        return
    
    lines = data.splitlines()
    
    # apply the fix
    try:
        find_and_fix_candidate_key_line(lines)
        log("CANDIDATE_KEY_LINE_FOUND!")
    except NoKeyException:
        log("No candidate key lines found, is there a key?")
        possibly_a_key = participant.iosdecryptionkey_set.filter(file_name=clean_file_path)
        if not possibly_a_key.exists():
            log("No.")
            return
        # there can only be one, enforced on the model
        created_on = possibly_a_key.get().created_on.astimezone(LOCAL_TIME_ZONE).strftime('%Y-%m-%d %I:%M:%S%p (%Z)')
        log(f"Yes! (from {created_on}.")
    
    STATS.DATA_PUSHED_OUT += len(lines)
    
    # stick the data back together
    possible_file_contents: bytes = "\n".join(lines).encode()
    try:
        decryptor = DeviceDataDecryptor(
            clean_file_path, possible_file_contents, participant, ignore_existing_keys=True, rsa_key=rsa_key
        )
        log("Decrypted without error!")
    except (DecryptionKeyInvalidError, IosDecryptionKeyNotFoundError, IosDecryptionKeyDuplicateError,
            RemoteDeleteFileScenario, UnHandledError, InvalidIV, InvalidData, DefinitelyInvalidFile) as e:
        log(f"Decryption error... it didn't work.")
        # log(f"{e} - decryption error... it didn't work.")
        
        # we might as well try without the ignore_existing_keys flag...
        try:
            decryptor = DeviceDataDecryptor(
                clean_file_path, possible_file_contents, participant, rsa_key=rsa_key
            )
            log(f"but the backup DID??!!!")
        except (DecryptionKeyInvalidError, IosDecryptionKeyNotFoundError, IosDecryptionKeyDuplicateError,
            RemoteDeleteFileScenario, UnHandledError, InvalidIV, InvalidData, DefinitelyInvalidFile) as e:
            # log(possible_file_contents)
            return
    
    # and then frequently there is a line before the header that is junk, probably encrypted in
    # the context of the previous file. We can find the true start of any csv by looking for 
    # 'timestamp,' and removing everything before that. (it will be binary noise)
    if not decryptor.decrypted_file:
        log("but it was an empty decrypted file...")  # probably unreachable
        return
    
    # this is slightly destructive and I'm not sure how to determine if it cuts out real data.
    # File processing should be able to handle junk lines that make it to this point.
    # try:
    #     final_file = b"timestamp," + decryptor.decrypted_file.split(b"timestamp,", 1)[1]
    #     log("timestamp normalization success! adding to processing pipeline!")
    #     upload_and_create_file_to_process_and_log(clean_file_path, participant, decryptor)
    #     # log(final_file[:1024])
    #     # log(final_file)
    # except IndexError:
    #     pass
    
    upload_and_create_file_to_process_and_log(clean_file_path, participant, decryptor)

# our main loop process files participant-by-participant... holy crap is it slow.
# Ok, distributing this might be necessary. At least there are no cross-participant dependencies so
# we could spawn processes for each participant. Testing on a t3.medium server was about 25%$ cpu
# usage for single threaded.  If we do per-participant we can also correctly re-run any failed files
# after assembling all the new keys.
for list_of_file_paths in get_some_problem_uploads_by_participant():
    process_a_participants_files(list_of_file_paths)

duration = (datetime.now() - t_start).total_seconds() / 60
print("\n\n\n DONE \n\n\n")
print("", STATS.FILES_EXAMINED, "files.")
print("Read in", STATS.DATA_READ_IN/ 1024 / 1023, "Megabytes across", STATS.FILES_EXAMINED, "files.")
print("Decrypted", STATS.DATA_PUSHED_OUT, "lines of data")
# truncate to 2 decimal places
print(f"Time taken {duration:.2f} minutes.")
