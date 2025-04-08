# this is a stub to run a single participant purge for targeting by the celery script runner, all
# the logic is in libs.participant_purge
from libs.participant_purge import run_next_queued_participant_data_deletion

def main():
    run_next_queued_participant_data_deletion()
