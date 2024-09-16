from __future__ import annotations

from typing import Optional

from django.db import models

from database.common_models import TimestampedModel
from database.user_models_researcher import Researcher
from database.validators import PASSWORD_VALIDATOR, STANDARD_BASE_64_VALIDATOR
from libs.utils.security_utils import (BadDjangoKeyFormatting, compare_password,
    django_password_components, generate_hash_and_salt, generate_random_bytestring,
    generate_random_string, to_django_password_components)


class ApiKey(TimestampedModel):
    DESIRED_ITERATIONS = 310000  # 2022 recommendation pbkdf2 iterations for sha256 is 310,000
    DESIRED_ALGORITHM = "sha256"
    
    access_key_id = models.CharField(max_length=64, unique=True, validators=[STANDARD_BASE_64_VALIDATOR])
    access_key_secret = models.CharField(max_length=256, validators=[PASSWORD_VALIDATOR], blank=True)
    
    is_active = models.BooleanField(default=True)
    last_used = models.DateTimeField(null=True, blank=True)
    researcher: Researcher = models.ForeignKey(Researcher, null=True, on_delete=models.SET_NULL, related_name="api_keys")
    readable_name = models.TextField(blank=True, default="")
    
    _access_key_secret_plaintext = None
    
    @classmethod
    def generate(cls, researcher: Researcher, **kwargs) -> ApiKey:
        """ Create ApiKey with newly generated credentials credentials. """
        # Generate the access key, secret key, generate the hash components of it
        secret_key = generate_random_bytestring(64)
        secret_hash, secret_salt = generate_hash_and_salt(
            cls.DESIRED_ALGORITHM, cls.DESIRED_ITERATIONS, secret_key
        )
        final_secret = to_django_password_components(
            cls.DESIRED_ALGORITHM, cls.DESIRED_ITERATIONS, secret_hash, secret_salt
        )
        api_key = cls.objects.create(
            access_key_id=generate_random_string(64),
            access_key_secret=final_secret,
            researcher=researcher,
            **kwargs,
        )
        api_key._access_key_secret_plaintext = secret_key.decode()  # part of a test
        return api_key
    
    @property
    def access_key_secret_plaintext(self) -> Optional[str]:
        """ Returns the value of the plaintext version of `access_key_secret` if it is cached on
        this instance and immediately deletes it. """
        plaintext = self._access_key_secret_plaintext
        if plaintext:
            del self._access_key_secret_plaintext
        return plaintext
    
    def update_secret_key(self, new_secret_key: str):
        secret_hash, secret_salt = generate_hash_and_salt(
            self.DESIRED_ALGORITHM, self.DESIRED_ITERATIONS, new_secret_key
        )
        self.access_key_secret = to_django_password_components(
            self.DESIRED_ALGORITHM, self.DESIRED_ITERATIONS, secret_hash, secret_salt
        )
        self.save()
    
    def proposed_secret_key_is_valid(self, proposed_secret_key: str) -> bool:
        """ Extract the current credential info, run comparison, will in-place-upgrade the existing
        password hash if there is a match """
        proposed_secret_key = proposed_secret_key.encode()  # needs to be a bytestring twice
        try:
            algorithm, iterations, current_password_hash, salt = django_password_components(self.access_key_secret)
        except BadDjangoKeyFormatting:
            return False
        
        it_matched = compare_password(
            algorithm, iterations, proposed_secret_key, current_password_hash, salt)
        # whenever we encounter an older password (THAT PASSES OLD-STYLE VALIDATION DUHURR!)
        # use the now-known-correct password value to apply the new-style password.
        if it_matched and (iterations != self.DESIRED_ITERATIONS or algorithm != self.DESIRED_ALGORITHM):
            self.update_secret_key(proposed_secret_key)
        return it_matched
