from csv import writer
from re import sub

from flask import Blueprint, flash, redirect, request, Response

from authentication.admin_authentication import authenticate_researcher_study_access
from database.schedule_models import InterventionDate
from database.study_models import Study
from database.user_models import Participant, ParticipantFieldValue
from libs.push_notification_config import repopulate_all_survey_scheduled_events
from libs.s3 import create_client_key_pair, s3_upload
from libs.streaming_bytes_io import StreamingStringsIO

participant_administration = Blueprint('participant_administration', __name__)


@participant_administration.route('/reset_participant_password', methods=["POST"])
@authenticate_researcher_study_access
def reset_participant_password():
    """ Takes a patient ID and resets its password. Returns the new random password."""
    patient_id = request.values['patient_id']
    study_id = request.values['study_id']

    try:
        participant = Participant.objects.get(patient_id=patient_id)
    except Participant.DoesNotExist:
        flash(f'The participant {patient_id} does not exist', 'danger')
        return redirect(f'/view_study/{study_id}/')

    if participant.study.id != int(study_id):
        flash(f'Participant {patient_id} is not in study {Study.objects.get(id=study_id).name}', 'danger')
        return redirect(request.referrer)

    new_password = participant.reset_password()
    flash(f'Patient {patient_id}\'s password has been reset to {new_password}.', 'success')
    return redirect(request.referrer)


@participant_administration.route('/reset_device', methods=["POST"])
@authenticate_researcher_study_access
def reset_device():
    """
    Resets a participant's device. The participant will not be able to connect until they
    register a new device.
    """

    patient_id = request.values['patient_id']
    study_id = request.values['study_id']

    try:
        participant = Participant.objects.get(patient_id=patient_id)
    except Participant.DoesNotExist:
        flash(f'The participant {patient_id} does not exist', 'danger')
        return redirect(f'/view_study/{study_id}/')

    if participant.study.id != int(study_id):
        flash(f'Participant {patient_id} is not in study {Study.objects.get(id=study_id).name}', 'danger')
        return redirect(request.referrer)

    participant.device_id = ""
    participant.save()
    flash(f'For patient {patient_id}, device was reset; password is untouched. ', 'success')
    return redirect(request.referrer)

@participant_administration.route('/unregister_participant', methods=["POST"])
@authenticate_researcher_study_access
def unregister_participant():
    """
    Block participant from uploading further data
    """

    patient_id = request.values['patient_id']
    study_id = request.values['study_id']

    try:
        participant = Participant.objects.get(patient_id=patient_id)
    except Participant.DoesNotExist:
        flash(f'The participant {patient_id} does not exist', 'danger')
        return redirect(f'/view_study/{study_id}/')

    if participant.study.id != int(study_id):
        flash(f'Participant {patient_id} is not in study {Study.objects.get(id=study_id).name}', 'danger')
        return redirect(request.referrer)

    if participant.unregistered:
        flash(f'Participant {patient_id} is already unregistered', 'danger')
        return redirect(request.referrer)

    participant.unregistered = True
    participant.save()
    flash(f'{patient_id} was successfully unregisted from the study. They will not be able to upload further data. ', 'danger')
    return redirect(request.referrer)


@participant_administration.route('/create_new_participant', methods=["POST"])
@authenticate_researcher_study_access
def create_new_participant():
    """
    Creates a new user, generates a password and keys, pushes data to s3 and user database, adds
    user to the study they are supposed to be attached to and returns a string containing
    password and patient id.
    """

    study_id = request.values['study_id']
    patient_id, password = Participant.create_with_password(study_id=study_id)
    participant = Participant.objects.get(patient_id=patient_id)
    study = Study.objects.get(id=study_id)
    add_fields_and_interventions(participant, study)

    # Create an empty file on S3 indicating that this user exists
    study_object_id = Study.objects.filter(pk=study_id).values_list('object_id', flat=True).get()
    s3_upload(patient_id, b"", study_object_id)
    create_client_key_pair(patient_id, study_object_id)
    repopulate_all_survey_scheduled_events(study, participant)

    response_string = 'Created a new patient\npatient_id: {:s}\npassword: {:s}'.format(patient_id, password)
    flash(response_string, 'success')

    return redirect('/view_study/{:s}'.format(study_id))


@participant_administration.route('/create_many_patients/<string:study_id>', methods=["POST"])
@authenticate_researcher_study_access
def create_many_patients(study_id=None):
    """ Creates a number of new users at once for a study.  Generates a password and keys for
    each one, pushes data to S3 and the user database, adds users to the study they're supposed
    to be attached to, and returns a CSV file for download with a mapping of Patient IDs and
    passwords. """
    number_of_new_patients = int(request.form.get('number_of_new_patients', 0))
    desired_filename = request.form.get('desired_filename', '')
    filename_spaces_to_underscores = sub(r'[\ =]', '_', desired_filename)
    filename = sub(r'[^a-zA-Z0-9_\.=]', '', filename_spaces_to_underscores)
    if not filename.endswith('.csv'):
        filename += ".csv"
    return Response(participant_csv_generator(study_id, number_of_new_patients),
                    mimetype="csv",
                    headers={'Content-Disposition': 'attachment; filename="%s"' % filename})


def participant_csv_generator(study_id, number_of_new_patients):
    study = Study.objects.get(pk=study_id)
    si = StreamingStringsIO()
    filewriter = writer(si)
    filewriter.writerow(['Patient ID', "Registration password"])

    for _ in range(number_of_new_patients):
        patient_id, password = Participant.create_with_password(study_id=study_id)
        participant = Participant.objects.get(patient_id=patient_id)
        add_fields_and_interventions(participant, Study.objects.get(id=study_id))
        # Creates an empty file on s3 indicating that this user exists
        s3_upload(patient_id, b"", study.object_id)
        create_client_key_pair(patient_id, study.object_id)
        repopulate_all_survey_scheduled_events(study, participant)

        filewriter.writerow([patient_id, password])
        yield si.getvalue()
        si.empty()


def add_fields_and_interventions(participant: Participant, study: Study):
    """ Creates empty ParticipantFieldValue and InterventionDate objects for newly created
     participants, doesn't affect existing instances. """
    for field in study.fields.all():
        ParticipantFieldValue.objects.get_or_create(participant=participant, field=field)
    for intervention in study.interventions.all():
        InterventionDate.objects.get_or_create(participant=participant, intervention=intervention)
