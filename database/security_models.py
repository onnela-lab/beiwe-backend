from typing import Optional

from django.db import models

from database.common_models import TimestampedModel
from database.user_models import Researcher
from database.validators import STANDARD_BASE_64_VALIDATOR, URL_SAFE_BASE_64_VALIDATOR
from libs.security import generate_random_string, generate_hash_and_salt, compare_password


class ApiKey(TimestampedModel):
    access_key_id = models.CharField(
        max_length=64, unique=True, validators=[STANDARD_BASE_64_VALIDATOR]
    )
    access_key_secret = models.CharField(max_length=44, validators=[URL_SAFE_BASE_64_VALIDATOR])
    access_key_secret_salt = models.CharField(
        max_length=24, validators=[URL_SAFE_BASE_64_VALIDATOR]
    )

    is_active = models.BooleanField(default=True)

    has_tableau_api_permissions = models.BooleanField(default=False)

    researcher = models.ForeignKey(Researcher, on_delete=models.PROTECT, related_name="api_keys")

    readable_name = models.TextField(blank=True, default="")

    _access_key_secret_plaintext = None

    @classmethod
    def generate(cls, researcher: Researcher, **kwargs) -> "ApiKey":
        """
        Create ApiKey with newly generated credentials credentials.
        """
        access_key = generate_random_string()[:64]
        secret_key = generate_random_string()[:64]
        secret_hash, secret_salt = generate_hash_and_salt(secret_key)

        api_key = cls.objects.create(
            access_key_id=access_key.decode(),
            access_key_secret=secret_hash.decode(),
            access_key_secret_salt=secret_salt.decode(),
            researcher=researcher,
            **kwargs,
        )
        api_key._access_key_secret_plaintext = secret_key.decode()
        return api_key

    @property
    def access_key_secret_plaintext(self) -> Optional[str]:
        """
        Returns the value of the plaintext version of `access_key_secret` if it is cached on this
        instance and immediately deletes it.
        """
        plaintext = self._access_key_secret_plaintext
        if plaintext:
            del self._access_key_secret_plaintext
        return plaintext

    def proposed_secret_key_is_valid(self, proposed_secret_key) -> bool:
        """
        Checks if the proposed secret key is valid for this ApiKey.
        """
        return compare_password(
            proposed_secret_key.encode(),
            self.access_key_secret_salt.encode(),
            self.access_key_secret.encode(),
        )



