# these are the fields required from a values query for use in the ZipGenerator class.

# ZipGenerator is used in the data access api, and in the download task data endpoint for forest.
CHUNK_FIELDS = (
    "pk", "participant_id", "data_type", "chunk_path", "time_bin", "chunk_hash",
    "participant__patient_id", "study_id", "survey_id", "survey__object_id"
)