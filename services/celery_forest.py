import json
import logging
import shutil
import traceback
from csv import DictReader
from datetime import date, datetime, timedelta
from os import makedirs
from os.path import dirname, exists as file_exists, join as path_join
from time import sleep

from dateutil.tz import UTC
from django.db.models import Sum
from django.utils import timezone

from constants.celery_constants import FOREST_QUEUE, ForestTaskStatus
from constants.common_constants import API_TIME_FORMAT, RUNNING_TESTS
from constants.forest_constants import (CLEANUP_ERROR as CLN_ERR, FOREST_TREE_REQUIRED_DATA_STREAMS,
    ForestTree, NO_DATA_ERROR, ROOT_FOREST_TASK_PATH, SYCAMORE_OUTPUT_COLUMN_NAMES_TO_FIELD_NAMES,
    TREE_COLUMN_NAMES_TO_SUMMARY_STATISTICS, YEAR_MONTH_DAY)
from constants.raw_data_constants import CHUNK_FIELDS
from database.models import (ChunkRegistry, ForestTask, ForestVersion, QuerySet,
    SummaryStatisticDaily, SycamoreAnalysisOutput)
from libs.celery_control import forest_celery_app, safe_apply_async
from libs.endpoint_helpers.copy_study_helpers import format_study
from libs.intervention_utils import intervention_survey_data, survey_history_export
from libs.s3 import s3_retrieve
from libs.sentry import SentryUtils
from libs.utils.date_utils import get_timezone_shortcode, legible_time
from libs.utils.file_name_utils import determine_base_file_name
from libs.utils.forest_utils import (save_all_bv_set_bytes, save_jasmine_all_memory_dict_bytes,
    save_output_file)
from libs.utils.threadpool_utils import s3_op_threaded_iterate

from forest.jasmine.traj2stats import gps_stats_main
from forest.oak.base import run as run_oak
from forest.sycamore.base import compute_survey_stats
from forest.willow.log_stats import log_stats_main


"""
This entire code path could be rewritten as a class, but all the data we need or want to track is
collected on the ForestTask object.  For code organization reasons the [overwhelming] majority of
code for running any given forest task should be in this file, not attached to the ForestTask
database model. File paths, constants, simple lookups, parameters should go on that class.
"""

MIN_TIME = datetime.min.time()
MAX_TIME = datetime.max.time()

logger = logging.getLogger("forest_runner")
logger.setLevel(logging.ERROR) if RUNNING_TESTS else logger.setLevel(logging.INFO)
log = logger.info
logw = logger.warning
loge = logger.error
logd = logger.debug

class NoSentryException(Exception): pass
class BadForestField(Exception): pass


TREE_TO_FOREST_FUNCTION = {  # points to the target runtime function
    ForestTree.jasmine: gps_stats_main,
    ForestTree.oak: run_oak,
    ForestTree.sycamore: compute_survey_stats,
    ForestTree.willow: log_stats_main,
}

#
## Celery and dev helpers
#

def enqueue_forest_task(**kwargs):
    expiry = (timezone.now().astimezone(UTC) + timedelta(minutes=5)).replace(second=30, microsecond=0)
    safe_apply_async(
        celery_run_forest,
        expires=expiry,
        max_retries=0,
        retry=False,
        task_publish_retry=False,
        task_track_started=True,
        **kwargs
    )


def create_forest_celery_tasks():
    """ Basic entrypoint, does what it says """
    pending_tasks = ForestTask.objects.filter(status=ForestTaskStatus.queued)
    with SentryUtils.report_forest():
        for task in pending_tasks:
            # always print
            print(
                f"Queueing up celery task for {task.participant} on tree {task.forest_tree} "
                f"from {task.data_date_start} to {task.data_date_end}"
            )
            enqueue_forest_task(args=[task.id])

#
## The forest task runtime
#

# @forest_celery_app.task(queue=FOREST_QUEUE)
def celery_run_forest(forest_task_id):
    
    task = ForestTask.objects.get(id=forest_task_id)
    
    if task.status != ForestTaskStatus.queued:
        logw(f"Task {task.external_id} has status {task.status}, exiting.")
        return
    
    # handle old sycamore reruns by removing the participant at runtime
    # if task.forest_tree == ForestTree.sycamore and task.participant is not None:
    #     task.update_only(participant=None)  # fails validation
    
    # there's a script that periodically updates the forest verison
    forest_version = ForestVersion.singleton()
    task.update_only(  # Set metadata on the task to running
        status=ForestTaskStatus.running,
        process_start_time=timezone.now(),
        forest_version=forest_version.package_version,
        forest_commit=forest_version.git_commit,
    )
    
    # ChunkRegistry "time_bin" hourly chunks are in UTC, with each file containing a discrete hour
    # of data. Manually entered data streams (like survey answers or media files) have a more
    # specific "time_bin" that is just the original file timestamp.
    # Our query for source data to uses the study's timezone such that starts of days align to local
    # midnight and local end-of-day to 11:59.59pm. (Those weird fractional timezones will be
    # noninclusive of the hour containing their first fractional offset, but inclusive of data in
    # the hour containing their last fractional offset. Manually entered data streams don't have
    # this issue.)
    # Code: construct two datetimes for the start and end of day in the study's timezone.
    starttime_midnight = datetime.combine(task.data_date_start, MIN_TIME, task.the_study.timezone)
    endtime_11_59pm = datetime.combine(task.data_date_end, MAX_TIME, task.the_study.timezone)
    log(f"starttime_midnight: {legible_time(starttime_midnight)}")
    log(f"endtime_11_59pm: {legible_time(endtime_11_59pm)}")
    
    # do the thing
    execute_forest_task_safe(task, starttime_midnight, endtime_11_59pm)


def execute_forest_task_safe(task: ForestTask, start: datetime, end: datetime):
    ## try-except 3 - clean up files. Report errors.
    # report cleanup operations cleanly to both sentry and forest task infrastructure.
    try:
        run_forest_task(task, start, end)
    finally:
        log("deleting files 2")
        try:
            clean_up_files(task)
        except Exception as e:
            # merging stack traces, handling null case, then conditionally report with tags
            task.update_only(stacktrace=((task.stacktrace or "") + CLN_ERR + traceback.format_exc()))
            log(f"task.stacktrace 2:\n{task.stacktrace}")
            with SentryUtils.report_forest(**task.sentry_tags):
                raise e from None


def run_forest_task(task: ForestTask, start: datetime, end: datetime):
    """ Given a time range, downloads all data and executes a tree on that data. """
    ## try-except 1 - the main work block. Download data, run Forest, upload any cache files.
    ## The except block handles reporting errors.
    try:
        download_data(task, start, end)
        run_one_forest_tree(task)
        upload_cache_files(task)
        task.update_only(status=ForestTaskStatus.success)
    
    except BaseException as e:
        error_repr = traceback.format_exc()
        task.update_only(status=ForestTaskStatus.error, stacktrace=error_repr)
        
        # only report errors that are not our special cases.
        if not isinstance(e, NoSentryException):
            print("task.stacktrace 1:\n", error_repr)
            with SentryUtils.report_forest(**task.sentry_tags):
                raise
    
    finally:
        # there won't be anything to run generate report on if there was no data.
        error_sentry = SentryUtils.report_forest(**task.sentry_tags)
        
        if not task.stacktrace or NO_DATA_ERROR not in task.stacktrace:
            
            try:
                generate_report(task)
            except Exception as e1:
                print(f"Something went wrong with report generation. {e1}")
                print(traceback.format_exc())
                with error_sentry:
                    raise
            
            try:
                compress_and_upload_raw_output(task)
            except Exception as e2:
                print(f"Something went wrong with saving task output. {e2}")
                print(traceback.format_exc())
                with error_sentry:
                    raise
    
    ## this is functionally a try-except block because all the above real try-except blocks
    ## re-raise their error inside an error sentry
    log(f"task.status: {task.status}")
    log("deleting files 1")
    clean_up_files(task)  # if this fails you probably have server oversubscription issues.
    task.update_only(process_end_time=timezone.now())


def run_one_forest_tree(task: ForestTask):
    caller_function = TREE_TO_FOREST_FUNCTION[task.forest_tree]
    
    # Run Forest
    params_dict = task.get_params_dict()
    log(f"params_dict: {params_dict}")
    task.pickle_to_pickled_parameters(params_dict)
    
    log(f"running: {task.forest_tree}")
    caller_function(**params_dict)
    log(f"done running: {task.forest_tree}")
    
    # Save data
    task.update_only(forest_output_exists=read_in_output_data(task))


#
## Reading in data and adding data to database
#

def read_in_output_data(task: ForestTask) -> bool:
    if task.forest_tree == ForestTree.sycamore:
        return read_in_sycamore_output(task)
    return read_in_summary_statistic_output(task)


## Summary statistics

def read_in_summary_statistic_output(task: ForestTask) -> bool:
    """ Construct summary statistics from forest output, returning whether or not any
        SummaryStatisticDaily has potentially been created or updated. """
    
    if not file_exists(task.summary_statistics_results_path):
        loge("summary statistics path does not exist:", task.summary_statistics_results_path)
        return False
    
    log(f"tree: {task.forest_tree}")
    with open(task.summary_statistics_results_path) as f:
        log(f"opened `{task.summary_statistics_results_path}`, parsing...")
        return summary_statistic_csv_parse_and_consume(task, DictReader(f))


def summary_statistic_csv_parse_and_consume(task: ForestTask, csv_reader: DictReader) -> bool:
    """ Parse a csv file and create/update SummaryStatisticDaily objects.
        This function can be mocked with a list of dicts for testing. """
    t = task
    
    blow_up_on_invalid_columns(task, csv_reader)
    taskname, participant, tree, study = t.taskname, t.participant, t.forest_tree, t.the_study
    timezone = study.timezone
    assert participant is not None, "this code path expects a participant on a study"
    
    assert (fieldnames := csv_reader.fieldnames) is not None  # yuck
    DATE = "Date" if "Date" in fieldnames else "date" if "date" in fieldnames else None
    if DATE is None:
        raise BadForestField("'Date'/'date' column is missing")
    
    rows_processed = 0
    for csv_row in csv_reader:
        summary_date = date.fromisoformat(csv_row[DATE])
        
        # if timestamp is outside of desired range, skip (use <=, this is inclusive)
        # (Really the scenario should never occurr where this is false, but we check anyway.)
        if not (task.data_date_start <= summary_date <= task.data_date_end):
            continue
        
        updates: dict = {
            taskname: task,
            "timezone": get_timezone_shortcode(summary_date, timezone),
        }
        
        # Extract the desied summary statistics from the csv row. Most columns in csvs have weird
        # names, we need to look up what the column name means in TREE_COLUMN_NAMES_TO_SUMMARY_STATISTICS
        # force Nones on no data fields, not empty strings (db table issue)
        # we don't need to do any column name checking, that was done in blow_up_on_invalid_columns
        for column_name, value in csv_row.items():
            if column_name in TREE_COLUMN_NAMES_TO_SUMMARY_STATISTICS:
                summary_stat_field = TREE_COLUMN_NAMES_TO_SUMMARY_STATISTICS[column_name]
                updates[summary_stat_field] = value if value != '' else None
        
        SummaryStatisticDaily.objects.update_or_create(
            date=summary_date, defaults=updates, participant=participant, the_study=study
        )
        rows_processed += 1
    
    log(f"update {rows_processed} SummaryStatisticDaily rows")
    return rows_processed > 0


def read_in_sycamore_output(task: ForestTask) -> bool:
    """ Constructs a new SycamoreAnalysisOutput from sycamore output data. """
    
    if not file_exists(task.sycamore_output_file):
        loge(f"sycamore path does not exist: {task.sycamore_output_file}")
        return False
    
    log(f"tree: {task.forest_tree}")
    with open(task.sycamore_output_file) as f:
        log(f"opened `{task.sycamore_output_file}`, parsing...")
        # returns True if it doesn't crash
        return sycamore_analysis_csv_parse_and_consume(task, DictReader(f))


def blow_up_on_invalid_columns(task: ForestTask, csv_reader: DictReader):
    assert csv_reader.fieldnames is not None
    assert task.forest_tree != ForestTree.sycamore, "incorrect validation function used for sycamore"
    valid_names = [*TREE_COLUMN_NAMES_TO_SUMMARY_STATISTICS, "Date", "date"]
    bad_names = []
    for column_name in csv_reader.fieldnames:
        # raise error on unrecognized column names. Data must be to spec.
        if column_name not in valid_names:
            bad_names.append(column_name)
    if bad_names:
        raise BadForestField("Bad Columns Encountered: " + ", ".join(bad_names))


#
## Sycamore (not a daily summary statistic) has it's own machinery
#


def sycamore_analysis_csv_parse_and_consume(task: ForestTask, csv_reader: DictReader):
    # file is guranteed to exist at this point
    sycamore_data = validate_sycamore_output(csv_reader)
    
    SycamoreAnalysisOutput(
        study=task.the_study,
        sycamore_task=task,
        source_data_start=task.data_date_start,
        source_data_end=task.data_date_end,
        **sycamore_data
    ).save()
    
    return True


def validate_sycamore_output(csv_reader: DictReader) -> dict[str, float]:
    ## Validate output - # file is guranteed to exist at this point
    
    all_rows = [row for row in csv_reader]
    #! fixme: this is probably wrong
    if len(all_rows) != 1:
        raise Exception(f"Sycamore output should only have one row, found {len(all_rows)}")
    
    row_dict = all_rows[0]
    
    bad_fields = []
    bad_values = {}
    for k, v in row_dict.items():
        if k not in SYCAMORE_OUTPUT_COLUMN_NAMES_TO_FIELD_NAMES:
            bad_fields.append(k)
            loge(f"Unrecognized column name in sycamore output: {k}, skipping.")
        
        try:
            _ = float(v)
        except ValueError:
            bad_values[k] = v
            loge(f"Invalid value for column `{k}` in sycamore output: `{v}`")
    
    if bad_fields:
        raise BadForestField(f"Found {len(bad_fields)} unrecognized columns in sycamore output: {bad_fields}")
    
    bad_fields = []
    for k in SYCAMORE_OUTPUT_COLUMN_NAMES_TO_FIELD_NAMES:
        if k not in row_dict:
            bad_fields.append(k)
            loge(f"Expected column {k} not found in sycamore output.")
    
    if bad_fields:
        raise BadForestField(f"Found {len(bad_fields)} missing columns in sycamore output: {bad_fields}")
    
    if bad_values:
        raise BadForestField(f"Found {len(bad_values)} columns with invalid values in sycamore output: {bad_values}")
    
    return {SYCAMORE_OUTPUT_COLUMN_NAMES_TO_FIELD_NAMES[k]: float(v) for k, v in row_dict.items()}


## Post-Run code


def generate_report(task: ForestTask):
    now = timezone.now()
    tz_name = task.the_study.timezone
    study = task.the_study
    patient_id = task.participant.patient_id if task.participant else "None"
    with open(task.task_report_path, "w") as f:
        f.write(f"Completed Forest task report\n\n")
        f.write(f"Generated at {legible_time(now)}\n\n")
        
        f.write(f"Participant: {patient_id}\n")
            
        f.write(f"Study: `{study.name}`\n")
        f.write(f"Study id: {study.object_id}\n")
        
        # Forest Datapoints
        f.write(f"Forest tree: {task.forest_tree}\n")
        f.write(f"Forest version: {task.forest_version}\n")
        f.write(f"Forest commit: {task.forest_commit}\n")
        f.write(f"Forest task id: {task.external_id}\n")
        
        # data information
        f.write(f"Data start date: {legible_time(task.data_date_start)}\n")
        f.write(f"Data end date: {legible_time(task.data_date_end)} (inclusive)\n")
        
        ## Everthing after this point is only available if the task was successful.
        # (total_file_size might be available if the task failed)
        if task.total_file_size:
            # file size in megabytes, 2 decimal places
            f.write(f"Total file size: {task.total_file_size / 1024 / 1024:.2f}MB\n")
        
        # time information
        p_start = task.process_start_time
        p_end = task.process_end_time
        p_download_end = task.process_download_end_time
        if p_start:
            f.write(f"Process start time: {legible_time(p_start)} ({legible_time(p_start.astimezone(tz_name))})\n")
        if p_download_end:
            f.write(f"Process download end time: {legible_time(p_download_end)} ({legible_time(p_download_end.astimezone(tz_name))})\n")
        if p_end:
            f.write(f"Process end time: {legible_time(p_end)} ({legible_time(p_end.astimezone(tz_name))})\n")
        
        # runtime details, stack traces and extra parameters
        if task.stacktrace:
            f.write("\n")
            f.write(f"This Forest task encountered an error:\n{task.stacktrace}\n")
        
        try:
            parameters_repr = repr(task.unpickle_from_pickled_parameters())
        except Exception as e:
            parameters_repr = f"Could not load parameters from database:\n{e}"
        f.write(f"\n\nPython representation of any extra parameters that were passed into the Forest tree:\n{parameters_repr}\n")


#
## Files
#

## File utility code

def clean_up_files(task: ForestTask):
    """ Delete temporary input and output files from this Forest run. """
    for i in range(10):
        try:
            shutil.rmtree(task.root_path_for_task)
        except OSError:  # this is pretty expansive, but there are an endless number of os errors...
            pass
        # file system can be slightly slow, we need to sleep. (this code never executes on frontend)
        sleep(0.5)
        if not file_exists(task.root_path_for_task):
            return
    raise Exception(
        f"Could not delete folder {task.root_path_for_task} for task {task.external_id}, tried {i} times."
    )


def ensure_folders_exist(task: ForestTask):
    """ This io is minimal, simply always make sure these folder structures exist. """
    makedirs(ROOT_FOREST_TASK_PATH, exist_ok=True)
    makedirs(task.root_path_for_task, exist_ok=True)
    # files
    makedirs(dirname(task.input_interventions_file), exist_ok=True)
    makedirs(dirname(task.input_study_config_file), exist_ok=True)
    # folders
    makedirs(task.data_input_path, exist_ok=True)
    makedirs(task.data_output_folder_path, exist_ok=True)
    makedirs(task.tree_base_path, exist_ok=True)


## Download data code


def download_data(task: ForestTask, start: datetime, end: datetime):
    chunks = ChunkRegistry.objects.filter(
        participant=task.participant,
        time_bin__gte=start,
        time_bin__lte=end,
        data_type__in=FOREST_TREE_REQUIRED_DATA_STREAMS[task.forest_tree]
    )
    file_size = chunks.aggregate(Sum('file_size')).get('file_size__sum')
    if file_size is None:
        raise NoSentryException(NO_DATA_ERROR)
    task.update_only(total_file_size=file_size)
    
    # Download data
    download_data_files(task, chunks)
    task.update_only(process_download_end_time=timezone.now())
    log(f"task.process_download_end_time: {legible_time(task.process_download_end_time)}")
    
    # get extra custom files for any trees that need them (currently just sycamore)
    if task.forest_tree == ForestTree.sycamore:
        get_interventions_data(task)
        get_study_config_data(task)
        get_survey_history_data(task)


def download_data_files(task: ForestTask, chunks: QuerySet[ChunkRegistry]) -> None:
    """ Download only the files needed for the forest task. """
    ensure_folders_exist(task)
    # this is an iterable, this is intentional, retain it.
    params = ((task, chunk) for chunk in chunks.values("study__object_id", *CHUNK_FIELDS))
    # and run!
    s3_op_threaded_iterate(batch_download_and_write_file, params)


def batch_download_and_write_file(task_and_chunk_tuple: tuple[ForestTask, dict]):
    """ Wrapper for basic file download operations so that it can be run in a ThreadPool. """
    
    # weird unpack of variables, do s3_retrieve.
    forest_task, chunk = task_and_chunk_tuple
    contents = s3_retrieve(chunk["chunk_path"], chunk["study__object_id"], raw_path=True)
    # file ops, sometimes we have to add folder structure (surveys)
    file_name = path_join(forest_task.data_input_path, determine_base_file_name(chunk))
    makedirs(dirname(file_name), exist_ok=True)
    
    try:
        with open(file_name, "xb") as f:
            f.write(contents)
    except FileExistsError:
        # we used to track this, it happens when someone uploads duplicate files, which we handle
        # but at some point we started deduplicating the file names so it retriggered. Silencing.
        # with SentryUtils.report_forest(file_name=file_name, **forest_task.sentry_tags):
        #     raise
        pass

## Study metadata files

def get_study_config_data(task: ForestTask):
    """ Puts a study config file for the participant's survey in a known location. """
    ensure_folders_exist(task)
    with open(task.input_study_config_file, "wb") as f:
        f.write(format_study(task.the_study))


def get_interventions_data(task: ForestTask):
    """ Puts a study's interventions file for the participant's survey in a known location. """
    ensure_folders_exist(task)
    with open(task.input_interventions_file, "w") as f:
        f.write(json.dumps(intervention_survey_data(task.the_study)))


def get_survey_history_data(task: ForestTask):
    """ Puts the study's survey history export in a known location. """
    ensure_folders_exist(task)
    history_json = survey_history_export(task.the_study)
    with open(task.input_survey_history_file, "wb") as f:
        f.write(history_json)


## Data Upload code


def compress_and_upload_raw_output(task: ForestTask):
    """ Compresses raw output files and uploads them to S3. """
    object_id = task.the_study.object_id
    tree_name = task.forest_tree
    
    base_file_name = f"{task.id}_{timezone.now().strftime(API_TIME_FORMAT)}_output"
    file_path = path_join(task.root_path_for_task, base_file_name)
    
    filename = shutil.make_archive(
        base_name=file_path,  # base_name is the zip file path minus the extension
        format="zip",  # its a zip
        root_dir=task.data_output_folder_path,  # the root directory of the zip file
    )
    # (this only ever runs on *nix, path_join is always correct)
    if task.forest_tree == ForestTree.sycamore:
        s3_path = f"{object_id}/{tree_name}/{task.external_id}/{base_file_name}.zip"
    else:
        base_file_name = f"{task.forest_tree}_{base_file_name}"
        s3_path = f"{object_id}/{task.participant.patient_id}/{base_file_name}.zip"
    
    task.update(output_zip_s3_path=s3_path)
    
    with open(filename, "rb") as f:
        save_output_file(task, f.read())


# Extras
def upload_cache_files(task: ForestTask):
    """ Find output files from forest tasks and consume them. """
    
    if file_exists(task.all_bv_set_path):
        with open(task.all_bv_set_path, "rb") as f:
            save_all_bv_set_bytes(task, f.read())
    
    if file_exists(task.all_memory_dict_path):
        with open(task.all_memory_dict_path, "rb") as f:
            save_jasmine_all_memory_dict_bytes(task, f.read())
