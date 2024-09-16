import bleach
from django import forms

from constants.forest_constants import (SERIALIZABLE_FIELD_NAMES,
    SERIALIZABLE_FIELD_NAMES_DROPDOWN, VALID_QUERY_PARAMETERS, ForestTree)
from constants.celery_constants import ForestTaskStatus
from database.forest_models import ForestTask
from database.study_models import Study
from database.user_models_participant import Participant
from libs.django_forms.form_fields import CommaSeparatedListCharField, CommaSeparatedListChoiceField


class NewApiKeyForm(forms.Form):
    readable_name = forms.CharField(required=False)
    
    def clean(self):
        super().clean()
    
    def clean_readable_name(self):
        return bleach.clean(self.cleaned_data["readable_name"])


class DisableApiKeyForm(forms.Form):
    api_key_id = forms.CharField()


class CreateTasksForm(forms.Form):
    date_start = forms.DateField()
    date_end = forms.DateField()
    participant_patient_ids = CommaSeparatedListCharField()  # not actually a comma separated field?
    trees = CommaSeparatedListChoiceField(choices=ForestTree.choices())
    
    def __init__(self, *args, **kwargs):
        # we provide a study parameter, somewhat like a ModelForm
        self.study: Study = kwargs.pop("study")
        super().__init__(*args, **kwargs)
    
    def clean(self):
        cleaned_data = super().clean()
        
        # handle cases of missing fields.
        if "date_end" not in cleaned_data:
            self.add_error("date_end", "date end was not provided.")
        if "date_start" not in cleaned_data:
            self.add_error("date_start", "date start was not provided.")
        if "date_end" not in cleaned_data or "date_start" not in cleaned_data:
            return
        
        if cleaned_data["date_end"] < cleaned_data["date_start"]:
            error_message = "Start date must be before or the same as end date."
            self.add_error("date_start", error_message)
            self.add_error("date_end", error_message)
    
    def clean_trees(self):
        # trees is required,  this code doesn't execute when it is missing. validity is already
        # checked by the CommaSeparatedListChoiceField, but we still need to use getlist to access
        # the many values in the multidict.
        return self.data.getlist("trees", [])
    
    def clean_participant_patient_ids(self):
        """ Filter participants to those who are registered in this study and specified in this
        field (instead of raising a ValidationError if an invalid or non-study patient id is
        specified). """
        # need to use getlist to access the many values in the multidict
        patient_ids = self.data.getlist("participant_patient_ids")
        participants = Participant.objects \
            .filter(patient_id__in=patient_ids, study=self.study).values("id", "patient_id")
        
        # get database ids and patient ids
        self.cleaned_data["participant_ids"] = [participant["id"] for participant in participants]
        return [participant["patient_id"] for participant in participants]
    
    def save(self):
        # generates forest task objects for each selected option.
        forest_tasks = []
        for participant_id in self.cleaned_data["participant_ids"]:
            for tree in self.cleaned_data["trees"]:
                forest_tasks.append(
                    ForestTask(
                        participant_id=participant_id,
                        forest_tree=tree,
                        data_date_start=self.cleaned_data["date_start"],
                        data_date_end=self.cleaned_data["date_end"],
                        status=ForestTaskStatus.queued,
                    )
                )
        ForestTask.objects.bulk_create(forest_tasks)


class ApiQueryForm(forms.Form):
    end_date = forms.DateField(
        required=False,
        error_messages={
            "invalid": "end date could not be interpreted as a date. Dates should be "
                       "formatted as YYYY-MM-DD"
        },
    )
    
    start_date = forms.DateField(
        required=False,
        error_messages={
            "invalid": "start date could not be interpreted as a date. Dates should be "
                       "formatted as YYYY-MM-DD"
        },
    )
    
    limit = forms.IntegerField(
        required=False,
        error_messages={"invalid": "limit value could not be interpreted as an integer value"},
    )
    ordered_by = forms.ChoiceField(
        choices=SERIALIZABLE_FIELD_NAMES_DROPDOWN,
        required=False,
        error_messages={
            "invalid_choice": "%(value)s is not a field that can be used to sort the output"
        },
    )
    
    order_direction = forms.ChoiceField(
        choices=[("ascending", "ascending"), ("descending", "descending")],
        required=False,
        error_messages={
            "invalid_choice": "If provided, the order_direction parameter "
                              "should contain either the value 'ascending' or 'descending'"
        },
    )
    
    participant_ids = CommaSeparatedListCharField(required=False)
    
    fields = CommaSeparatedListChoiceField(
        choices=SERIALIZABLE_FIELD_NAMES_DROPDOWN,
        default=SERIALIZABLE_FIELD_NAMES,
        required=False,
        error_messages={"invalid_choice": "%(value)s is not a valid field"},
    )
    
    def clean(self) -> dict:
        """ Retains only members of VALID_QUERY_PARAMETERS and non-falsey-but-not-False objects """
        super().clean()
        return {
            k: v for k, v in self.cleaned_data.items()
            if k in VALID_QUERY_PARAMETERS and (v or v is False)
        }


class StudySecuritySettingsForm(forms.ModelForm):
    class Meta:
        fields = [
            "password_minimum_length", "password_max_age_enabled", "password_max_age_days", "mfa_required"
        ]
        model = Study
    
    password_max_age_enabled = forms.CheckboxInput()
    mfa_required = forms.CheckboxInput()


class ParticipantExperimentForm(forms.ModelForm):
    EXPERIMENT_DESCRIPTIONS = {
        # "enable_heartbeat": "The server will send a notification to the participant asking them to open the app if the device has not checked in for over an hour.",
        "enable_extensive_device_info_tracking": "Store device info every time the device makes an API call to the server. (This can generate a lot of data, it is for development and debugging purposes only.)",
    }
    
    class Meta:
        fields = Participant.EXPERIMENT_FIELDS
        model = Participant
    
    # (hackily) iterate over EXPERIMENT_FIELDS and set its widget to CheckboxInput
    for field in Participant.EXPERIMENT_FIELDS:
        locals()[field] = forms.CheckboxInput()
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name, description in self.EXPERIMENT_DESCRIPTIONS.items():
            self.fields[field_name].help_text = description


class StudyEndDateForm(forms.Form):
    end_date = forms.DateField(required=False)
