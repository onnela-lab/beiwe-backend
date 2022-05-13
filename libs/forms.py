import datetime

import pytz
from django import forms

from database.schedule_models import ParticipantMessage, ParticipantMessageScheduleType


class ParticipantMessageForm(forms.ModelForm):
    schedule_type = forms.ChoiceField(
        choices=ParticipantMessageScheduleType.choices(),
        initial=ParticipantMessageScheduleType.asap,
    )
    date = forms.DateField(required=False)
    message = forms.CharField()
    time = forms.TimeField(required=False)
    
    # intervention = forms.ModelChoiceField(queryset=Intervention.objects.none(), required=False)
    
    class Meta:
        model = ParticipantMessage
        fields = [
            "message",
            "schedule_type",
        ]
    
    def __init__(self, *args, **kwargs):
        """
        Note: This form discards the values of any non-required fields based on the schedule_type.
        """
        self.participant = kwargs.pop("participant")
        super().__init__(*args, **kwargs)
        # self.fields["intervention"].queryset = Intervention.objects.filter(
        #     study__participants=self.participant,
        # )

    def clean(self):
        for field_name in ["date", "time"]:
            if self.cleaned_data.get("schedule_type") == ParticipantMessageScheduleType.asap:
                # If ASAP, we can ignore these fields even if they have errors
                if field_name in self.errors:
                    del self.errors[field_name]
                if field_name in self.cleaned_data:
                    del self.cleaned_data[field_name]
            else:
                self._validate_field_as_required(field_name)
    
    def save(self, commit=True):
        super().save(commit=False)
        self.instance.participant = self.participant
        if self.cleaned_data["schedule_type"] == ParticipantMessageScheduleType.absolute:
            send_datetime_naive = datetime.datetime.combine(self.cleaned_data["date"], self.cleaned_data["time"])
            send_datetime_utc = (
                pytz.timezone(self.participant.study.timezone_name)
                    .localize(send_datetime_naive)
                    .astimezone(pytz.utc)
            )
            self.instance.scheduled_send_datetime = send_datetime_utc
        if commit:
            self.instance.save()
    
    def _validate_field_as_required(self, field_name: str):
        """
        Mark fields as required and revalidate required.
        """
        self.fields[field_name].required = True
        if self.cleaned_data.get(field_name) is None and field_name not in self.errors:
            self.add_error(field_name, self.fields[field_name].error_messages["required"])
    
    # def clean(self):
    #     # No default value needed since required=True
    #     schedule_type = self.cleaned_data.get("schedule_type")
    #     if schedule_type == ParticipantMessageScheduleType.asap:
    #         self.cleaned_data["scheduled_send_datetime"] = timezone.now()
    
    # def save(self):
    #     # Prune self.cleaned_data to only keep required fields
    #     self.cleaned_data = {
    #         field_name: self.cleaned_data[field_name]
    #         for field_name, field in self.fields.items()
    #         if field.required
    #     }
    #
    # def _post_clean(self):
    #     super()._post_clean()
    #
    #     # Remove errors for non-required fields
    #     error_keys_to_delete = []
    #     for field_name, error in self.errors.items():
    #         if not self.fields[field_name].required:
    #             error_keys_to_delete.append(field_name)
    #     for error_key in error_keys_to_delete:
    #         del self.errors[error_key]
