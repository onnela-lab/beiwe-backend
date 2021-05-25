from django import forms
from django.core.exceptions import ValidationError


class CommaSeparatedListFieldMixin(forms.Field):
    """ A mixin for use with django form fields. This mixin changes the field to accept a comma separated list of
        inputs that are individually cleaned and validated. Takes one optional parameter, list_validators, which is
        a list of validators to be applied to the final list of values (the validator parameter still expects a single
        value as input, and is applied to each value individually) """
    NONSTRING_ERROR_MESSAGE = "Please supply only string arguments to a CommaSeparatedListField"

    def __init__(self, *args, default=None, **kwargs):
        self.default = default if default is not None else []
        super().__init__(*args, **kwargs)

    def clean(self, value) -> list:
        errors = []
        
        if not value:
            if self.required:
                raise ValidationError(self.error_messages['required'], code='required')
            else:
                return self.default
                
        if not isinstance(value, str):
            raise ValidationError(self.NONSTRING_ERROR_MESSAGE)
        
        value_list = value.split(",")

        cleaned_values = []
        for v in value_list:
            try:
                cleaned_values.append(super().clean(v.strip()))
            except ValidationError as err:
                errors.append(err)

        if errors:
            raise ValidationError(errors)

        return cleaned_values


class CommaSeparatedListCharField(CommaSeparatedListFieldMixin, forms.CharField):
    pass


class CommaSeparatedListChoiceField(CommaSeparatedListFieldMixin, forms.ChoiceField):
    pass
