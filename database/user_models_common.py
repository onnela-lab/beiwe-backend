from __future__ import annotations

from typing import Tuple

from django.db import models

from database.models import TimestampedModel
from database.validators import PASSWORD_VALIDATOR
from libs.utils.security_utils import (BadDjangoKeyFormatting, compare_password,
    django_password_components, generate_easy_alphanumeric_string, generate_hash_and_salt,
    to_django_password_components)


class AbstractPasswordUser(TimestampedModel):
    """ The AbstractPasswordUser (APU) model is used to enable basic password functionality for
    human users of the database, whatever variety of user they may be.

    APU descendants have passwords hashed once with sha256 and many times (as defined in
    settings.py) with PBKDF2, and salted using a cryptographically secure random number generator.
    The sha256 check duplicates the storage of the password on the mobile device, so that the APU's
    password is never stored in a reversible manner. """
    DESIRED_ALGORITHM = None
    DESIRED_ITERATIONS = None
    
    password = models.CharField(max_length=256, validators=[PASSWORD_VALIDATOR])
    
    def generate_hash_and_salt(self, password: bytes) -> Tuple[bytes, bytes]:
        return generate_hash_and_salt(self.DESIRED_ALGORITHM, self.DESIRED_ITERATIONS, password)
    
    def set_password(self, password: str):
        """ Sets the instance's password hash to match the hash of the provided string. """
        password_hash, salt = self.generate_hash_and_salt(password.encode())
        # march 2020: this started failing when running postgres in a local environment.  There
        # appears to be some extra type conversion going on, characters are getting expanded when
        # passed in as bytes, causing failures in passing length validation.
        # -- this was caused by the new django behavior that casts bytestrings to their string
        #    representation silently.  Fix is to insert decode statements
        self.password = to_django_password_components(
            self.DESIRED_ALGORITHM, self.DESIRED_ITERATIONS, password_hash, salt
        )
        self.save()
    
    def reset_password(self) -> str:
        """ Resets the patient's password to match an sha256 hash of a randomly generated string. """
        password = generate_easy_alphanumeric_string()
        self.set_password(password)
        return password
    
    def validate_password(self, compare_me: str) -> bool:
        """ Extract the current password info, run comparison, will in-place-upgrade the existing 
        password hash if there is a match """
        try:
            algorithm, iterations, current_password_hash, salt = django_password_components(self.password)
        except BadDjangoKeyFormatting:
            return False
        
        it_matched = compare_password(algorithm, iterations, compare_me.encode(), current_password_hash, salt)
        # whenever we encounter an older password (THAT PASSES OLD-STYLE VALIDATION DUHURR!)
        # use the now-known-correct password value to apply the new-style password.
        if it_matched and (iterations != self.DESIRED_ITERATIONS or algorithm != self.DESIRED_ALGORITHM):
            self.set_password(compare_me)
        return it_matched
    
    class Meta:
        abstract = True
