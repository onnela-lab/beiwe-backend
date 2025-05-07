from django.core.exceptions import ValidationError
from django.core.validators import RegexValidator
from django.utils.deconstruct import deconstructible


@deconstructible
class LengthValidator:
    # Oh good, this is referenced in the migrations so it can never be removed
    
    length = None  # If length is None, no validation is done
    message = 'Ensure this value has exactly {} characters (it has {}).'
    code = 'invalid'
    
    def __init__(self, length=None, message=None, code=None):
        if length is not None:
            self.length = length
        if message is not None:
            self.message = message
        if code is not None:
            self.code = code
    
    def __call__(self, value):
        """Validate that the input is of the proper length, otherwise raise ValidationError."""
        if self.length is not None and len(value) != self.length:
            message = self.message.format(self.length, len(value))
            raise ValidationError(message, code=self.code)
    
    def __eq__(self, other):
        return (
            isinstance(other, LengthValidator)
            and self.length == other.length
            and self.message == other.message
            and self.code == other.code
        )
    
    def __ne__(self, other):
        return not (self == other)


# These validators are used by CharFields in the Researcher and Participant models to ensure that
# those fields' values fit the given regex. The max length requirement is handled by the CharField,
# but the validator ensures that only certain characters are present in the field value. If the ID
# or hashes are changed, be sure to modify or create a new validator accordingly.
ID_VALIDATOR = RegexValidator('^[1-9a-z]+$', message='This field can only contain characters 1-9 and a-z.')
# Base 64 encodings can end in up to two = symbols for padding.
_b64_chars = "[0-9a-zA-Z+/]"
_b64_chars_with_padding = _b64_chars + "+={0,2}"
_b64_chars_url = r"[0-9a-zA-Z_\-]"
_b64_chars_url_with_padding = _b64_chars_url + "+={0,2}"
_valid_algorithms = "(sha1|sha256)"

URL_SAFE_BASE_64_VALIDATOR = RegexValidator(f'^{_b64_chars_url_with_padding}$')
STANDARD_BASE_64_VALIDATOR = RegexValidator(f'^{_b64_chars_with_padding}$')

# the django password format is "alorithm$iterations$password_hash_in_b64$password_salt_in_b64"
# we currently differ only in that we were using url safe b64 for... reasons.
PASSWORD_VALIDATOR = RegexValidator(
    f"^{_valid_algorithms}\$[0-9]+\${_b64_chars_url_with_padding}\${_b64_chars_url_with_padding}$"
)


# base32 is obscure and uses this obscure alphabet (see wikipedia page, RFC 4648 version)
_b32_chars = "[2-7A-Z]"
_b32_chars_with_padding = _b32_chars + "+={0,6}"
B32_VALIDATOR = RegexValidator(_b32_chars_with_padding)