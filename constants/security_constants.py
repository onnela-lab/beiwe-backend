import string


## Password Check Regexes
SYMBOL_REGEX = "[^a-zA-Z0-9]"
LOWERCASE_REGEX = "[a-z]"
UPPERCASE_REGEX = "[A-Z]"
NUMBER_REGEX = "[0-9]"
PASSWORD_REQUIREMENT_REGEX_LIST = [SYMBOL_REGEX, LOWERCASE_REGEX, UPPERCASE_REGEX, NUMBER_REGEX]

EASY_ALPHANUMERIC_CHARS = string.ascii_lowercase + '123456789'  # intentionally does not have 0

BASE64_GENERIC_ALLOWED_CHARACTERS = string .ascii_lowercase + string.ascii_uppercase + string.digits + "/+"
OBJECT_ID_ALLOWED_CHARS = string.ascii_uppercase + string.ascii_lowercase + string.digits

ASYMMETRIC_KEY_LENGTH = 2048  # length of private/public keys

# this is a set of integers (bytes, technically), it is faster than testing bytes
URLSAFE_BASE64_CHARACTERS = set(b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-=")

MFA_CREATED = "mfa_created"