from constants.data_stream_constants import AUDIO_RECORDING, SURVEY_ANSWERS, SURVEY_TIMINGS
from libs.utils.security_utils import generate_easy_alphanumeric_string


SURVEY_ID_STREAMS = (SURVEY_ANSWERS, SURVEY_TIMINGS, AUDIO_RECORDING)


def generate_duplicate_name(s3_file_path: str):
    """ when duplicates occur we add this string onto the end and try to proceed as normal. """
    return s3_file_path + "-duplicate-" + generate_easy_alphanumeric_string(10)


def normalize_duplicate_section(s3_file_path: str):
    """ clear the duplicate section from the file name """
    
    if "-duplicate-" in s3_file_path:
        return s3_file_path.split("-duplicate-")[0]
    
    return s3_file_path


def determine_base_file_name(chunk: dict) -> str:
    """ Generates the correct file name to provide the file with in the zip file.
        (This also includes the folder location files in the zip.) """
    chunk_path = chunk["chunk_path"]
    
    validate_inputs(chunk_path)
    
    patient_id = chunk["participant__patient_id"]
    # time_bin = str(chunk["time_bin"]).replace(":", "_")  # why would/wouldn't it be a string...?
    time_bin = chunk["time_bin"].isoformat().replace(":", "_").replace("T", " ")
    data_stream = chunk["data_type"]
    
    survey_id = get_survey_id_from_chunk(chunk)
    
    return build_file_name(chunk_path, data_stream, patient_id, time_bin, survey_id)


def build_file_name(
    path: str, data_stream: str, patient_id: str, time_str: str, survey_id: str | None = None
) -> str:
    
    path = normalize_duplicate_section(path)
    
    if data_stream in (SURVEY_ANSWERS, SURVEY_TIMINGS):
        return f"{patient_id}/{data_stream}/{survey_id}/{time_str}.csv"
    
    if data_stream == AUDIO_RECORDING:
        extension = get_extension(path)  # extension can be .wav or .mp4
        return f"{patient_id}/{data_stream}/{survey_id}/{time_str}.{extension}"
    
    return f"{patient_id}/{data_stream}/{time_str}.csv"  # everything else


def validate_inputs(chunk_path: str) -> None:
    
    if chunk_path.count(".") != 1:
        raise ValueError(f"chunk_path should have exactly one '.' in it, received '{chunk_path}'")
    
    if chunk_path.count("/") < 3:
        raise ValueError(f"chunk_path should have at least three '/' in it, received '{chunk_path}'")


def get_extension(chunk_path: str) -> str:
    # Some paths may have alphanumeric extensions ottached to them for storage-side deduplication purposes.
    # This is unusual but can happen due to app bugs or crashes so we can't reeaallly get rid of it.
    extension = chunk_path.rsplit(".", 1)[1]
    extension = extension[-3:]
    return extension


# old
def get_survey_id_from_chunk(chunk: dict) -> str:
    survey_id = chunk.get("survey__object_id")
    if not survey_id:
        # example real path as it should come in: 5873fe38644ad7557b168e43/q41aozrx/voiceRecording/587442edf7321c14da193487/1524857988384.wav
        survey_id = chunk["chunk_path"].rsplit("/", 2)[1]
        if len(survey_id) != 24:
            return "unknown_survey_id"  # if there still isn't a survey input unknown?
    return survey_id


# def get_survey_id(chunk_path: str) -> str:
#     # example real path as it should come in: 5873fe38644ad7557b168e43/q41aozrx/voiceRecording/587442edf7321c14da193487/1524857988384.wav
#     survey_id = chunk_path.rsplit("/", 2)[1]
#     if len(survey_id) != 24:
#         return "unknown_survey_id"  # if there still isn't a survey input unknown?
#     return survey_id
