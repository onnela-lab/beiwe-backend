from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, tzinfo
from pprint import pprint
from time import sleep
from typing import Dict, List, Tuple, Union

from dateutil.tz import gettz
from django.utils import timezone
from django.utils.timezone import localtime

from constants.action_log_messages import HEARTBEAT_PUSH_NOTIFICATION_SENT
from constants.common_constants import DEV_TIME_FORMAT, DEV_TIME_FORMAT3
from constants.message_strings import MESSAGE_SEND_SUCCESS
from database.data_access_models import FileToProcess
from database.profiling_models import UploadTracking
from database.schedule_models import ArchivedEvent, ScheduledEvent
from database.study_models import Study
from database.survey_models import Survey
from database.user_models_participant import Participant, SurveyNotificationReport
from database.user_models_researcher import Researcher
from libs.s3 import s3_list_files
from libs.utils.dev_utils import disambiguate_participant_survey, TxtClr


#
## This file is referenced directly inside of developer documentation, keep it organized
## and well commented.
#

# a timezone that is used in as_local and for general usage in the shell.
# We should probably make this shell util configurable...
THE_ONE_TRUE_TIMEZONE = gettz("America/New_York")
EASTERN = THE_ONE_TRUE_TIMEZONE
EST = THE_ONE_TRUE_TIMEZONE
EDT = THE_ONE_TRUE_TIMEZONE


def as_local(dt: datetime, tz=THE_ONE_TRUE_TIMEZONE):
    """ Takes a datetime object and returns it in a useful timezone, defaults to THE_ONE_TRUE_TIMEZONE. """
    return localtime(dt, tz)


def tformat(dt: datetime, tz=THE_ONE_TRUE_TIMEZONE):
    """ Takes a datateme, returns a legible string representation using as_local. """
    return datetime.strftime(as_local(dt, tz), DEV_TIME_FORMAT3)


def PARTICIPANT(patient_id: Union[str, int]):
    """ Get a Participant, may use a contains match, also supports primary key integers. """
    if isinstance(patient_id, int):
        return Participant.objects.get(pk=patient_id)
    try:
        return Participant.objects.get(patient_id=patient_id)
    except Participant.DoesNotExist:
        participants = Participant.fltr(patient_id__icontains=patient_id)
        if participants.count() == 0:
            raise Participant.DoesNotExist() from None
        if participants.count() == 1:
            return participants.get()
        pprint(list(participants.values_list("patient_id", "id")))


P = PARTICIPANT  # Shorthand for PARTICIPANT, just type p = P("someone") and you are done


def RESEARCHER(username: Union[str, int]):
    """ Get a Researcher, may use a contains match, also supports primary key integers. """
    if isinstance(username, int):
        return Researcher.objects.get(pk=username)
    try:
        return Researcher.objects.get(username=username)
    except Researcher.DoesNotExist:
        researchers = Researcher.fltr(username__icontains=username)
        if researchers.count() == 0:
            raise Researcher.DoesNotExist() from None
        if researchers.count() == 1:
            return researchers.get()
        pprint(list(researchers.values_list("name", "id")))

R = RESEARCHER  # Shorthand for RESEARCHER, just type r = R("someone") and you are done


def SURVEY(id_or_name: Union[str, int]):
    """ Get a Survey, can be a website-style key, a primary key, or a name on a contains match. """
    if isinstance(id_or_name, int):
        ret = Survey.objects.get(pk=id_or_name)
    else:
        try:
            ret = Survey.objects.get(object_id=id_or_name)
        except Survey.DoesNotExist:
            surveys = Survey.fltr(name__icontains=id_or_name)
            if surveys.count() == 0:
                raise Survey.DoesNotExist() from None
            if surveys.count() == 1:
                return surveys.get()
            pprint(list(surveys.values_list("name", "id")))
    
    if ret.name:
        print(ret.name)
    else:
        print(ret.object_id)
    return ret


def STUDY(id_or_name: Union[str, int]):
    """ Get a Study, can be a website-style key, a primary key, or a name on a contains match. """
    if isinstance(id_or_name, int):
        ret = Study.objects.get(pk=id_or_name)
    else:
        try:
            ret = Study.objects.get(object_id=id_or_name)
        except Study.DoesNotExist:
            studies = Study.fltr(name__icontains=id_or_name)
            count = studies.count()
            if count == 1:
                return studies.get()
            if count < 1:
                raise Study.DoesNotExist() from None
            pprint(list(studies.values_list("name", "id")))
            return None
    
    print(ret.name)
    return ret


def file_process_count():
    """ Gets the number of files that are waiting to be processed. """
    return FileToProcess.objects.count()


def file_process_status():
    """ provides a short summary of the number of files to process per participant. """
    pprint(
        sorted(Counter(FileToProcess.objects.values_list("participant__patient_id", flat=True))
               .most_common(), key=lambda x: x[1])
    )


def watch_processing():
    """ Only works on data processing servers.
    Runs a loop that prints out the number of files to process, and some information about the
    current celery task queue for processing files. ctrl+c to stop. """
    
    # cannot be imported on EB servers
    from libs.celery_control import (CeleryNotRunningException, get_processing_active_job_ids,
        get_processing_reserved_job_ids, get_processing_scheduled_job_ids)
    
    periodicity = 5
    orig_start = localtime()
    a_now = orig_start
    s_now = orig_start
    r_now = orig_start
    active = []
    scheduled = []
    registered = []
    prior_users = 0
    
    for i in range(2**64):
        errors = 0
        start = localtime()
        
        count = FileToProcess.objects.count()
        user_count = FileToProcess.objects.values_list("participant__patient_id",
                                                       flat=True).distinct().count()
        
        if prior_users != user_count:
            print(f"{start:} Number of participants with files to process: {user_count}")
        
        print(f"{start}: {count} files to process")
        
        try:
            a_now, active = localtime(), get_processing_active_job_ids()
        except CeleryNotRunningException:
            errors += 1
        try:
            s_now, scheduled = localtime(), get_processing_scheduled_job_ids()
        except CeleryNotRunningException:
            errors += 1
        try:
            r_now, registered = localtime(), get_processing_reserved_job_ids()
        except CeleryNotRunningException:
            errors += 1
        
        if errors:
            # if this is always happening it means the data processing servers are either totally
            # overloaded, or there are no celery worker processes running.
            print(f"  (Couldn't connect to celery on {errors} attempt(s), data is slightly stale.)")
        
        print(a_now, "active tasks:", active)
        print(s_now, "scheduled tasks:", scheduled)
        print(r_now, "registered tasks:", registered)
        
        prior_users = user_count
        
        # we will set a minimum time between info updates, database call can be slow.
        end = localtime()
        total = abs((start - end).total_seconds())
        wait = periodicity - total if periodicity - total > 0 else 0
        
        print("\n=================================\n")
        sleep(wait)


def watch_uploads():
    """ Runs a loop that prints out the number of files uploaded in the past minute. ctrl+c to stop."""
    while True:
        start = localtime()
        data = list(UploadTracking.fltr(
            timestamp__gte=(start - timedelta(minutes=1))).values_list("file_size", flat=True))
        end = localtime()
        total = abs((start - end).total_seconds())
        
        # we will set a minimum time between prints at 2 seconds, database call can be slow.
        wait = 2 - total if 0 < (2 - total) < 2 else 0
        
        print("time delta: %ss, %s files, %.4fMB in the past minute" % (
            total + wait, len(data), (sum(data) / 1024.0 / 1024.0)))
        sleep(wait)


def watch_celery():
    """ Only works on data processing servers.
    Runs a loop that prints out the number of active, scheduled, and reserved tasks in celery. """
    from libs.celery_control import watch_celery as watch_celery2
    watch_celery2()


def get_and_summarize(patient_id: str):
    """ Get a participant and summarize their data uploads and files to process. """
    p = Participant.objects.get(patient_id=patient_id)
    byte_sum = sum(UploadTracking.fltr(participant=p).values_list("file_size", flat=True))
    print(f"Total Data Uploaded: {byte_sum/1024/1024}MB")
    
    counter = Counter(
        path.split("/")[2] for path in
        FileToProcess.fltr(participant=p).values_list("s3_file_path", flat=True)
    )
    return counter.most_common()


@disambiguate_participant_survey
def find_notification_events(
        participant: Participant = None,
        survey: Survey or str = None,
        schedule_type: str = None,
        tz: tzinfo = gettz('America/New_York'),
        flat=False
    ):
    """ Provides a information about historical notification events for a participant or survey.
    (largely superceded by the participant notifications page.

    Throw in a participant and or survey object, OR THEIR IDENTIFYING STRING and we make it work

    'survey_type'  will filter by survey type, duh.
    'flat'         disables alternating line colors.
    'tz'           will normalize timestamps to that timezone, default is us east.
    """
    filters = {}
    if participant:
        filters['participant'] = participant
    if schedule_type:
        filters["schedule_type"] = schedule_type
    if survey:
        filters["survey_archive__survey"] = survey
    elif participant:  # if no survey, yes participant:
        filters["survey_archive__survey__in"] = participant.study.surveys.all()
    
    # order by participant to separate out the core related events, then order by survey
    # to group the participant's related events together, and do this in order of most recent
    # at the top of all sub-lists.
    query = ArchivedEvent.fltr(**filters).order_by(
        "participant__patient_id", "survey_archive__survey__object_id", "-created_on")
    
    print(f"There were {query.count()} sent scheduled events matching your query.")
    participant_name = ""
    survey_id = ""
    for a in query:
        # only print participant name and survey id when it changes
        if a.participant.patient_id != participant_name:
            print(f"\nparticipant {TxtClr.CYAN}{a.participant.patient_id}{TxtClr.BLACK}:")
            participant_name = a.participant.patient_id
        if a.survey.object_id != survey_id:
            print(f"{a.survey.survey_type} {TxtClr.CYAN}{a.survey.object_id}{TxtClr.BLACK}:")
            survey_id = a.survey.object_id
        
        # data points of interest for sending information
        sched_time = localtime(a.scheduled_time, tz)
        sent_time = localtime(a.created_on, tz)
        time_diff_minutes = (sent_time - sched_time).total_seconds() / 60
        sched_time_print = datetime.strftime(sched_time, DEV_TIME_FORMAT)
        sent_time_print = datetime.strftime(sent_time, DEV_TIME_FORMAT)
        
        print(
            f"  {a.schedule_type} FOR {TxtClr.GREEN}{sched_time_print}{TxtClr.BLACK} "
            f"SENT {TxtClr.GREEN}{sent_time_print}{TxtClr.BLACK}  "
            f"\u0394 of {time_diff_minutes:.1f} min",
            end="",
            # \u0394 is the delta character
        )
        
        if a.status == MESSAGE_SEND_SUCCESS:
            print(f'  status: "{TxtClr.GREEN}{a.status}{TxtClr.BLACK}"')
        else:
            print(f'  status: "{TxtClr.YELLOW}{a.status}{TxtClr.BLACK}"')
        
        if not flat:
            # these lines get hard to read, color helps, we can alternate brightness like this!
            TxtClr.brightness_swap()


@disambiguate_participant_survey
def find_pending_events(
        participant: Participant = None,
        survey: Survey or str = None,
        tz: tzinfo = gettz('America/New_York'),
):
    """ Provides a information about PENDING notification events for a participant or survey.

    Throw in a participant and or survey object, OR THEIR IDENTIFYING STRING and we make it work
    'tz' will normalize timestamps to that timezone, default is us east.
    """
    # this is a simplified, modified version ofg the find_notification_events on ArchivedEvent.
    filters = {}
    if participant:
        filters['participant'] = participant
    if survey:
        filters["survey"] = survey
    elif participant:  # if no survey, yes participant:
        filters["survey__in"] = participant.study.surveys.all()
    
    query = ScheduledEvent.fltr(**filters).order_by(
        "survey__object_id", "participant__patient_id", "-scheduled_time", "-created_on"
    )
    survey_id = ""
    for a in query:
        # only print participant name and survey id when it changes
        if a.survey.object_id != survey_id:
            print(f"{a.survey.survey_type} {TxtClr.CYAN}{a.survey.object_id}{TxtClr.BLACK}:")
            survey_id = a.survey.object_id
        
        # data points of interest for sending information
        sched_time = localtime(a.scheduled_time, tz)
        sched_time_print = datetime.strftime(sched_time, DEV_TIME_FORMAT)
        print(
            f"  {a.get_schedule_type()} FOR {TxtClr.CYAN}{a.participant.patient_id}{TxtClr.BLACK}"
            f" AT {TxtClr.GREEN}{sched_time_print}{TxtClr.BLACK}",
        )


@disambiguate_participant_survey
def find_scheduled_events_with_no_notification_checkin(
    participant: Participant = None,
    survey: Survey or str = None,
    schedule_type: str = None,
    tz: tzinfo = gettz('America/New_York'),
):
    filters = {}
    if participant:
        filters['participant'] = participant
    if schedule_type:
        filters["schedule_type"] = schedule_type
    if survey:
        filters["survey_archive__survey"] = survey
    
    query = ScheduledEvent.fltr(**filters).order_by(
        "survey__object_id", "participant__patient_id", "-scheduled_time", "-created_on"
    )
    
    participant_pk_to_notification_uuids = SurveyNotificationReport.make_lookup_dict_list(
        {'participant_id': participant.pk} if participant else {},
        "participant_id",
        "notification_uuid",
    )
    _common_print(query, participant_pk_to_notification_uuids, tz)


@disambiguate_participant_survey
def find_archived_events_with_no_notification_checkin(
    participant: Participant = None,
    survey: Survey or str = None,
    schedule_type: str = None,
    tz: tzinfo = gettz('America/New_York'),
):
    filters = {}
    if participant:
        filters['participant'] = participant
    if schedule_type:
        filters["schedule_type"] = schedule_type
    if survey:
        filters["survey_archive__survey"] = survey
    
    query = ArchivedEvent.fltr(**filters).order_by(
        "survey_archive__survey__object_id", "participant__patient_id", "-scheduled_time", "-created_on"
    )
    
    participant_pk_to_notification_uuids = SurveyNotificationReport.make_lookup_dict_list(
        {'participant_id': participant.pk} if participant else {},
        "participant_id",
        "notification_uuid",
    )
    _common_print(query, participant_pk_to_notification_uuids, tz)


def _common_print(query, participant_pk_to_notification_uuids, tz):
    prior_survey_id = ""
    prior_p_pk = None
    x: Union[ScheduledEvent, ArchivedEvent]
    for x in query:
        schedule_type = x.get_schedule_type() if isinstance(x, ScheduledEvent) else x.schedule_type
        # only print participant name and survey id when it changes
        if x.participant_id != prior_p_pk:
            print(f"\nparticipant {TxtClr.CYAN}{x.participant.patient_id}{TxtClr.BLACK}:")
            prior_p_pk = x.participant.pk
        if x.survey.object_id != prior_survey_id:
            print(f"{x.survey.survey_type} {TxtClr.CYAN}{x.survey.object_id}{TxtClr.BLACK}:")
            prior_survey_id = x.survey.object_id
        
        exists = "  (exists)" if x.uuid in participant_pk_to_notification_uuids[x.participant_id] else " "
        
        # data points of interest for sending information
        sched_time = localtime(x.scheduled_time, tz)
        sched_time_print = datetime.strftime(sched_time, DEV_TIME_FORMAT)
        created = localtime(x.created_on, tz).strftime(DEV_TIME_FORMAT)
        print(
            exists,
            f"{schedule_type} FOR {TxtClr.CYAN}{x.participant.patient_id}{TxtClr.BLACK}"
            f" AT {TxtClr.GREEN}{sched_time_print}{TxtClr.BLACK}"
            f" with uuid {x.uuid} created {created}"
        )


def heartbeat_summary(p: Participant, max_age: int = 12):
    """ Get the heartbeat timestamps and push notification events, print out all the timestamps in
    day-by-day and hour-by-hour sections print statements, and print time deltas since the
    previous received heartbeat event. """
    # (this code quality is terrible becaus it was cobbled together as-needed.)
    max_age = (timezone.now() - timedelta(hours=max_age)).replace(minute=0, second=0, microsecond=0)
    
    # queries
    heartbeats_query = p.heartbeats.order_by("timestamp") \
        .filter(timestamp__gte=max_age) \
        .values_list("timestamp", "message")
    heartbeat_notifications = p.action_logs.order_by("timestamp") \
        .filter(action=HEARTBEAT_PUSH_NOTIFICATION_SENT, timestamp__gte=max_age) \
        .values_list("timestamp", flat=True)
    
    # insert events, add a timedelta of difference with the previous event.
    events: List[Tuple[datetime, Union[timedelta, str]]] = []
    for i, (t, message) in enumerate(heartbeats_query):
        events.append((
            as_local(t),
            timedelta(seconds=0) if i == 0 else t - events[i-1][0],  # force first a delta to 0.
            message
        ))
    
    if not events:
        print(f"No heartbeats found in the last {int} hours.")
        return
    
    # add the push notification events, second object in the tuple as a string, then re-sort.
    for t in heartbeat_notifications:
        events.append((as_local(t), "heartbeat notification sent.", ""))
    events.sort(key=lambda x: x[0])
    
    # group into days
    events_by_day = defaultdict(list)
    for t, delta_or_message, message in events:
       events_by_day[t.date()].append((t, delta_or_message, message))
    
    # got type signature?
    events_by_day: Dict[date, List[Tuple[datetime, Union[timedelta, str]]]] = dict(events_by_day)
    
    # initialize previous day to the first day
    prev_day = events[0][0].strftime('%Y-%m-%d')
    for day, day_data in events_by_day.items():
        
        # print the day header if it's a new day
        if day.strftime('%Y-%m-%d') != prev_day:
            prev_day = day.strftime('%Y-%m-%d')
            print(f"\n[{prev_day}]")
        
        for hour in range(24):
            # filter out events that are not in this hour
            one_hours_data = [(hb, delta_or_message, message) for (hb, delta_or_message, message) in day_data if hb.hour == hour]
            if not one_hours_data:
                continue
            
            # print the hour header if there are events in that hour
            print(f"  {hour:02}:00 - {hour:02}:59")
            
            # print each event in that hour, timedeltas are printed in seconds and minutes. 
            for t, delta_or_message, message in one_hours_data:
                if isinstance(delta_or_message, timedelta):
                    s = delta_or_message.total_seconds()
                    print(f"    {t.strftime('%H:%M:%S')} (Δ {s:.1f} sec, {s/60:.1f} min), - {message}")
                else:
                    print(f"    {t.strftime('%H:%M:%S')} - {delta_or_message}")
            print()
    
    final_timestamp = events[-1][0]
    print(
        f"and it has been {(timezone.now() - final_timestamp).total_seconds() / 60:.1f} "
        "minutes since that last event."
    )


def describe_problem_uploads():
    """ To-be-removed see https://github.com/onnela-lab/beiwe-backend/issues/360 """
    # path is a string that looks like this, including those extra 10 characters at the end:
    #    PROBLEM_UPLOADS/5873fe38644ad7557b168e43/c3b7mk7j/gps/1664315342657.csvQWERTYUIOP
    participant_counts = defaultdict(int)
    study_counts = defaultdict(int)
    print("counting files...")
    
    # may be hundreds of thousands of files
    for count, path in enumerate(s3_list_files("PROBLEM_UPLOADS", as_generator=True)):
        if count % 10000 == 0:
            print(count, "...", end="", sep="", flush=True)
        
        patient_id = path.split("/")[2]
        study_object_id = path.split("/")[1]
        participant_counts[patient_id] += 1
        study_counts[study_object_id] += 1
    
    print("\n")
    print("total number of files:", count)
    print()
    participant_counts = dict(participant_counts)
    study_counts = dict(study_counts)
    from pprint import pprint
    print("participant file counts:")
    pprint(dict(sorted(list(participant_counts.items()), key=lambda x: x[1], reverse=True)), sort_dicts=False)
    print()
    print("study file counts:")
    pprint(dict(sorted(list(study_counts.items()), key=lambda x: x[1], reverse=True)), sort_dicts=False)


def diff_strings(s1: str, s2: str):
    """ A function for comparing two strings, it will print out the text to be compared up to the
    first non-matching and then some characters on either side of it. Mostly useful in developing
    tests. """
    if isinstance(s1, bytes):
        s1 = s1.decode("utf-8")
    if isinstance(s2, bytes):
        s2 = s2.decode("utf-8")
    
    print()
    for i, (char_s1, char_s2) in enumerate(zip(s1, s2)):
        if char_s1 == char_s2:
            print(char_s1.encode('unicode_escape').decode(), end="")
        else:
            print("\n")
            print(f"diff at {i}, '{char_s1.encode('unicode_escape').decode()}' != '{char_s2.encode('unicode_escape').decode()}'")
            break
        
    if i == len(s1) - 1:
        print("\nstrings match!")
    else:
        print("")
        start = i - 20 if i - 20 > 0 else 0
        print(f"chars -20 to +20 of s1: '{s1[start:i+20].encode('unicode_escape').decode()}'")
        print(f"chars -20 to +20 of s2: '{s2[start:i+20].encode('unicode_escape').decode()}'")

string_diff = diff_strings  # I have never once remembered which name I used.
