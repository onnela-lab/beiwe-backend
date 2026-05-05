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
from django.db import transaction
from django.db.models import Sum
from django.utils import timezone

from constants.celery_constants import FOREST_QUEUE, ForestTaskStatus
from constants.common_constants import API_TIME_FORMAT, RUNNING_TESTS
from constants.forest_constants import (CLEANUP_ERROR as CLN_ERR, FOREST_TREE_REQUIRED_DATA_STREAMS,
    ForestTree, NO_DATA_ERROR, ROOT_FOREST_TASK_PATH, SYCAMORE_OUTPUT_COLUMN_NAMES_TO_FIELD_NAMES,
    TREE_COLUMN_NAMES_TO_SUMMARY_STATISTICS, YEAR_MONTH_DAY)
from constants.raw_data_constants import CHUNK_FIELDS
from database.models import (ChunkRegistry, ForestTask, ForestVersion, Participant, QuerySet,
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


# a lookup for pointing to the correct function for each tree (we need to look up by tree name)
TREE_TO_FOREST_FUNCTION = {
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

@forest_celery_app.task(queue=FOREST_QUEUE)
def celery_run_forest(forest_task_id):
    
    # this use of transaction.atomic should blockmultiple tasks from running at once - I don't think
    # our usage of celery is such that we need to worry about this.
    with transaction.atomic():
        forest_task = ForestTask.objects.get(id=forest_task_id)
        participant: Participant = forest_task.participant
        
        # Check if there already is a running task for this participant and tree, handling
        # concurrency and requeuing of the ask if necessary (locks db rows until end of transaction)
        tasks = ForestTask.objects.select_for_update() \
                .filter(participant=participant, forest_tree=forest_task.forest_tree)
        
        # if any other forest tasks are running, exit.
        if tasks.filter(status=ForestTaskStatus.running).exists():
            return
        
        # Get the chronologically earliest task that's queued
        forest_task = tasks.order_by("-data_date_start").first()
        
        if forest_task is None:  # Should be unreachable...
            return
        
        # there's a script that periodically updates the forest verison
        forest_version = ForestVersion.singleton()
        forest_task.update_only(  # Set metadata on the task to running
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
    starttime_midnight = datetime.combine(forest_task.data_date_start, MIN_TIME, forest_task.the_study.timezone)
    endtime_11_59pm = datetime.combine(forest_task.data_date_end, MAX_TIME, forest_task.the_study.timezone)
    log("starttime_midnight: ", starttime_midnight.isoformat())
    log("endtime_11_59pm: ", endtime_11_59pm.isoformat())
    
    # do the thing
    execute_forest_task_safe(forest_task, starttime_midnight, endtime_11_59pm)


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
            log("task.stacktrace 2:", task.stacktrace)
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
    log("task.status:", task.status)
    log("deleting files 1")
    clean_up_files(task)  # if this fails you probably have server oversubscription issues.
    task.update_only(process_end_time=timezone.now())


def run_one_forest_tree(forest_task: ForestTask):
    caller_function = TREE_TO_FOREST_FUNCTION[forest_task.forest_tree]
    
    # Run Forest
    params_dict = forest_task.get_params_dict()
    log("params_dict:", params_dict)
    forest_task.pickle_to_pickled_parameters(params_dict)
    
    log("running:", forest_task.forest_tree)
    caller_function(**params_dict)
    log("done running:", forest_task.forest_tree)
    
    # Save data
    forest_task.update_only(forest_output_exists=read_in_output_data(forest_task))


#
## Reading in data and adding data to database
#

def read_in_output_data(forest_task: ForestTask) -> bool:
    if forest_task.forest_tree == ForestTree.sycamore:
        return read_in_sycamore_output(forest_task)
    return read_in_summary_statistic_output(forest_task)


## Summary statistics

def read_in_summary_statistic_output(forest_task: ForestTask) -> bool:
    """ Construct summary statistics from forest output, returning whether or not any
        SummaryStatisticDaily has potentially been created or updated. """
    
    if not file_exists(forest_task.summary_statistics_results_path):
        loge("summary statistics path does not exist:", forest_task.summary_statistics_results_path)
        return False
    
    log("tree:", forest_task.forest_tree)
    with transaction.atomic(), open(forest_task.summary_statistics_results_path) as f:
        log(f"opened `{forest_task.summary_statistics_results_path}`, parsing...")
        return summary_statistic_csv_parse_and_consume(forest_task, DictReader(f))


def summary_statistic_csv_parse_and_consume(forest_task: ForestTask, csv_reader: DictReader) -> bool:
    """ Parse a csv file and create/update SummaryStatisticDaily objects.
        This function can be mocked with a list of dicts for testing. """
    col_name: str
    col_value: str
    
    blow_up_on_invalid_columns(forest_task, csv_reader)
    taskname = forest_task.taskname
    participant = forest_task.participant
    study = participant.study
    timezone = study.timezone
    tree = forest_task.forest_tree
    rows_processed = 0
    
    for csv_row in csv_reader:
        if tree == ForestTree.oak:
            # Oak has a different output format, it is a json file.
            summary_date = date.fromisoformat(csv_row['date'])
        else:
            summary_date = date(
                int(float(csv_row['year'])), int(float(csv_row['month'])), int(float(csv_row['day']))
            )
        
        # if timestamp is outside of desired range, skip (use <=, this is inclusive)
        # (Really the scenario should never occurr where this is false, but we check anyway.)
        if not (forest_task.data_date_start <= summary_date <= forest_task.data_date_end):
            continue
        
        updates: dict = {
            taskname: forest_task,
            "timezone": get_timezone_shortcode(summary_date, timezone),
        }
        
        # Extract the desied summary statistics from the csv row. Most columns in csvs have weird
        # names, we need to look up what the column name means in TREE_COLUMN_NAMES_TO_SUMMARY_STATISTICS
        # force Nones on no data fields, not empty strings (db table issue)
        # we don't need to do any column name checking, that was done in blow_up_on_invalid_columns
        for col_name, col_value in csv_row.items():
            if not (summary_stat_field := TREE_COLUMN_NAMES_TO_SUMMARY_STATISTICS[col_name]):
                continue
            updates[summary_stat_field] = col_value if col_value != '' else None
        
        # TODO: this is probably slow, can we do a bulk update_or_create?
        SummaryStatisticDaily.objects.update_or_create(
            date=summary_date, defaults=updates, participant=participant, the_study=study
        )
        rows_processed += 1
    
    log(f"update {rows_processed} SummaryStatisticDaily rows")
    return rows_processed > 0


def read_in_sycamore_output(forest_task: ForestTask) -> bool:
    """ Constructs a new SycamoreAnalysisOutput from sycamore output data. """
    
    if not file_exists(forest_task.sycamore_output_file):
        loge("sycamore path does not exist:", forest_task.sycamore_output_file)
        return False
    
    log("tree:", forest_task.forest_tree)
    with transaction.atomic(), open(forest_task.sycamore_output_file) as f:
        log(f"opened `{forest_task.sycamore_output_file}`, parsing...")
        # returns True if it doesn't crash
        return sycamore_analysis_csv_parse_and_consume(forest_task, DictReader(f))


def sycamore_csv_parse_and_consume(forest_task: ForestTask, csv_reader: DictReader):
    # validate the output of sycamore, update or create the relevant DB object
    participant = forest_task.participant
    study = participant.study
    rows = list[dict[str, str]](csv_reader)
    assert len(rows) == 1, f"Sycamore output should only have one row, found {len(rows)}"
    
    updates = {}
    for col_name, col_value in rows[0].items():
        
        if not (sycamore_field := SYCAMORE_OUTPUT_COLUMN_NAMES_TO_FIELD_NAMES.get(col_name)):
            logw(f"Unrecognized column name in sycamore output: {col_name}, skipping.")
            continue
        
        updates[sycamore_field] = col_value if col_value != '' else None
    
    SummaryStatisticDaily.objects.update_or_create(
        defaults=updates, participant=participant, the_study=study
    )


def blow_up_on_invalid_columns(forest_task: ForestTask, csv_reader: DictReader):
    assert csv_reader.fieldnames is not None
    for column_name in csv_reader.fieldnames:
        # raise error on unrecognized column names. Data must be to spec.
        if column_name not in TREE_COLUMN_NAMES_TO_SUMMARY_STATISTICS:
            if column_name not in YEAR_MONTH_DAY and column_name != "date":
                raise BadForestField(column_name)


#
## Sycamore (not a daily summary statistic) has it's own machinery
#


def sycamore_analysis_csv_parse_and_consume(forest_task: ForestTask, csv_reader: DictReader):
    # file is guranteed to exist at this point
    sycamore_data = validate_sycamore_output(csv_reader)
    
    SycamoreAnalysisOutput(
        study=forest_task.the_study,
        sycamore_task=forest_task,
        source_data_start=forest_task.data_date_start,
        source_data_end=forest_task.data_date_end,
        **sycamore_data
    ).save()
    
    return True


def blow_up_on_bad_sycamore_columns(forest_task: ForestTask, csv_reader: DictReader):
    assert csv_reader.fieldnames is not None
    for column_name in csv_reader.fieldnames:
        # raise error on unrecognized column names. Data must be to spec.
        if column_name not in SYCAMORE_OUTPUT_COLUMN_NAMES_TO_FIELD_NAMES:
            raise BadForestField(column_name)


def validate_sycamore_output(csv_reader: DictReader) -> dict[str, float]:
    ## Validate output - # file is guranteed to exist at this point
    
    all_rows = [row for row in csv_reader]
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
        raise Exception(f"Found {len(bad_fields)} unrecognized columns in sycamore output: {bad_fields}")
    
    bad_fields = []
    for k in SYCAMORE_OUTPUT_COLUMN_NAMES_TO_FIELD_NAMES:
        if k not in row_dict:
            bad_fields.append(k)
            loge(f"Expected column {k} not found in sycamore output.")
    
    if bad_fields:
        raise Exception(f"Found {len(bad_fields)} missing columns in sycamore output: {bad_fields}")
    
    if bad_values:
        raise Exception(f"Found {len(bad_values)} columns with invalid values in sycamore output: {bad_values}")
    
    return {k.lower().replace(" ", "_"): float(v) for k, v in row_dict.items()}


## Post-Run code


def generate_report(forest_task: ForestTask):
    now = timezone.now()
    tz_name = forest_task.the_study.timezone
    with open(forest_task.task_report_path, "w") as f:
        f.write(f"Completed Forest task report for {forest_task.participant.patient_id} on {legible_time(now)}\n")
        
        # Forest Datapoints
        f.write(f"Forest tree: {forest_task.forest_tree}\n")
        f.write(f"Forest version: {forest_task.forest_version}\n")
        f.write(f"Forest commit: {forest_task.forest_commit}\n")
        f.write(f"Forest task id: {forest_task.external_id}\n")
        
        # data information
        f.write(f"Data start date: {forest_task.data_date_start}\n")
        f.write(f"Data end date: {forest_task.data_date_end} (inclusive)\n")
        
        ## Everthing after this point is only available if the task was successful.
        # (total_file_size might be available if the task failed)
        if forest_task.total_file_size:
            # file size in megabytes, 2 decimal places
            f.write(f"Total file size: {forest_task.total_file_size / 1024 / 1024:.2f}MB\n")
        
        # time information
        p_start = forest_task.process_start_time
        p_end = forest_task.process_end_time
        p_download_end = forest_task.process_download_end_time
        if p_start:
            f.write(f"Process start time: {legible_time(p_start)} ({legible_time(p_start.astimezone(tz_name))})\n")
        if p_download_end:
            f.write(f"Process download end time: {legible_time(p_download_end)} ({legible_time(p_download_end.astimezone(tz_name))})\n")
        if p_end:
            f.write(f"Process end time: {legible_time(p_end)} ({legible_time(p_end.astimezone(tz_name))})\n")
        
        # runtime details, stack traces and extra parameters
        if forest_task.stacktrace:
            f.write("\n")
            f.write(f"This Forest task encountered an error:\n{forest_task.stacktrace}\n")
        
        try:
            parameters_repr = repr(forest_task.unpickle_from_pickled_parameters())
        except Exception as e:
            parameters_repr = f"Could not load parameters from database:\n{e}"
        f.write(f"\n\nPython representation of any extra parameters that were passed into the Forest tree:\n{parameters_repr}\n")


#
## Files
#

## File utility code

def clean_up_files(forest_task: ForestTask):
    """ Delete temporary input and output files from this Forest run. """
    for i in range(10):
        try:
            shutil.rmtree(forest_task.root_path_for_task)
        except OSError:  # this is pretty expansive, but there are an endless number of os errors...
            pass
        # file system can be slightly slow, we need to sleep. (this code never executes on frontend)
        sleep(0.5)
        if not file_exists(forest_task.root_path_for_task):
            return
    raise Exception(
        f"Could not delete folder {forest_task.root_path_for_task} for task {forest_task.external_id}, tried {i} times."
    )


def ensure_folders_exist(forest_task: ForestTask):
    """ This io is minimal, simply always make sure these folder structures exist. """
    makedirs(ROOT_FOREST_TASK_PATH, exist_ok=True)
    makedirs(forest_task.root_path_for_task, exist_ok=True)
    # files
    makedirs(dirname(forest_task.input_interventions_file), exist_ok=True)
    makedirs(dirname(forest_task.input_study_config_file), exist_ok=True)
    # folders
    makedirs(forest_task.data_input_path, exist_ok=True)
    makedirs(forest_task.data_output_folder_path, exist_ok=True)
    makedirs(forest_task.tree_base_path, exist_ok=True)


## Download data code


def download_data(forest_task: ForestTask, start: datetime, end: datetime):
    chunks = ChunkRegistry.objects.filter(
        participant=forest_task.participant,
        time_bin__gte=start,
        time_bin__lte=end,
        data_type__in=FOREST_TREE_REQUIRED_DATA_STREAMS[forest_task.forest_tree]
    )
    file_size = chunks.aggregate(Sum('file_size')).get('file_size__sum')
    if file_size is None:
        raise NoSentryException(NO_DATA_ERROR)
    forest_task.update_only(total_file_size=file_size)
    
    # Download data
    download_data_files(forest_task, chunks)
    forest_task.update_only(process_download_end_time=timezone.now())
    log("task.process_download_end_time:", forest_task.process_download_end_time.isoformat())
    
    # get extra custom files for any trees that need them (currently just sycamore)
    if forest_task.forest_tree == ForestTree.sycamore:
        get_interventions_data(forest_task)
        get_study_config_data(forest_task)
        get_survey_history_data(forest_task)


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

def get_study_config_data(forest_task: ForestTask):
    """ Puts a study config file for the participant's survey in a known location. """
    ensure_folders_exist(forest_task)
    with open(forest_task.input_study_config_file, "wb") as f:
        f.write(format_study(forest_task.the_study))


def get_interventions_data(forest_task: ForestTask):
    """ Puts a study's interventions file for the participant's survey in a known location. """
    ensure_folders_exist(forest_task)
    with open(forest_task.input_interventions_file, "w") as f:
        f.write(json.dumps(intervention_survey_data(forest_task.the_study)))


def get_survey_history_data(forest_task: ForestTask):
    """ Puts the study's survey history export in a known location. """
    ensure_folders_exist(forest_task)
    history_json = survey_history_export(forest_task.the_study)
    with open(forest_task.input_survey_history_file, "wb") as f:
        f.write(history_json)


## Data Upload code


def compress_and_upload_raw_output(forest_task: ForestTask):
    """ Compresses raw output files and uploads them to S3. """
    object_id = forest_task.the_study.object_id
    tree_name = forest_task.forest_tree
    
    base_file_name = f"{forest_task.id}_{timezone.now().strftime(API_TIME_FORMAT)}_output"
    file_path = path_join(forest_task.root_path_for_task, base_file_name)
    
    filename = shutil.make_archive(
        base_name=file_path,  # base_name is the zip file path minus the extension
        format="zip",  # its a zip
        root_dir=forest_task.data_output_folder_path,  # the root directory of the zip file
    )
    # (this only ever runs on *nix, path_join is always correct)
    if forest_task.participant is None:
        s3_path = f"{object_id}/{tree_name}/{forest_task.external_id}/{base_file_name}.zip"
    else:
        base_file_name = f"{forest_task.forest_tree}_{base_file_name}"
        s3_path = f"{object_id}/{forest_task.participant.patient_id}/{base_file_name}.zip"
    
    forest_task.update(output_zip_s3_path=s3_path)
    
    with open(filename, "rb") as f:
        save_output_file(forest_task, f.read())


# Extras
def upload_cache_files(forest_task: ForestTask):
    """ Find output files from forest tasks and consume them. """
    
    if file_exists(forest_task.all_bv_set_path):
        with open(forest_task.all_bv_set_path, "rb") as f:
            save_all_bv_set_bytes(forest_task, f.read())
    
    if file_exists(forest_task.all_memory_dict_path):
        with open(forest_task.all_memory_dict_path, "rb") as f:
            save_jasmine_all_memory_dict_bytes(forest_task, f.read())
