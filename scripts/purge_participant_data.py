# this is a stub to run a single participant purge for targeting by the celery script runner, all
# the logic is in libs.participant_purge
from libs.participant_purge import run_next_participant_data_deletion

def main():
    while True:
        patient_id = run_next_participant_data_deletion()
        if patient_id is None:
            break
        print(f"Finished deletion for participant {patient_id}")
