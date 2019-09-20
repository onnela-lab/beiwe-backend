from django.db import models
from django.db.models import F, Func

from config.constants import ResearcherRole
from database.models import AbstractModel
from database.validators import id_validator, standard_base_64_validator, url_safe_base_64_validator
from libs.security import (compare_password, device_hash, generate_easy_alphanumeric_string,
    generate_hash_and_salt, generate_random_string, generate_user_hash_and_salt)


class AbstractPasswordUser(AbstractModel):
    """
    The AbstractPasswordUser (APU) model is used to enable basic password functionality for human
    users of the database, whatever variety of user they may be.

    APU descendants have passwords hashed once with sha256 and many times (as defined in
    settings.py) with PBKDF2, and salted using a cryptographically secure random number
    generator. The sha256 check duplicates the storage of the password on the mobile device, so
    that the APU's password is never stored in a reversible manner.
    """

    password = models.CharField(max_length=44, validators=[url_safe_base_64_validator],
                                help_text='A hash of the user\'s password')
    salt = models.CharField(max_length=24, validators=[url_safe_base_64_validator])

    # This stub function declaration is present because it is used in the set_password funcion below
    def generate_hash_and_salt(self, password):
        """
        Generate a password hash and random salt from a given password. This is different
        for different types of APUs, depending on whether they use mobile or web.
        """
        raise NotImplementedError

    def set_password(self, password):
        """
        Sets the instance's password hash to match the hash of the provided string.
        """
        password_hash, salt = self.generate_hash_and_salt(password)
        self.password = password_hash
        self.salt = salt
        self.save()

    def reset_password(self):
        """
        Resets the patient's password to match an sha256 hash of a randomly generated string.
        """
        password = generate_easy_alphanumeric_string()
        self.set_password(password)
        return password

    def validate_password(self, compare_me):
        """
        Checks if the input matches the instance's password hash.
        """
        return compare_password(compare_me, self.salt, self.password)

    class Meta:
        abstract = True


class Participant(AbstractPasswordUser):
    """
    The Participant database object contains the password hashes and unique usernames of any
    participants in the study, as well as information about the device the participant is using.
    A Participant uses mobile, so their passwords are hashed accordingly.
    """
    
    IOS_API = "IOS"
    ANDROID_API = "ANDROID"
    NULL_OS = ''
    
    OS_TYPE_CHOICES = (
        (IOS_API, IOS_API),
        (ANDROID_API, ANDROID_API),
        (NULL_OS, NULL_OS),
    )

    patient_id = models.CharField(max_length=8, unique=True, validators=[id_validator],
                                  help_text='Eight-character unique ID with characters chosen from 1-9 and a-z')

    device_id = models.CharField(max_length=256, blank=True,
                                 help_text='The ID of the device that the participant is using for the study, if any.')
    os_type = models.CharField(max_length=16, choices=OS_TYPE_CHOICES, blank=True,
                               help_text='The type of device the participant is using, if any.')

    study = models.ForeignKey('Study', on_delete=models.PROTECT, related_name='participants', null=False)

    @classmethod
    def create_with_password(cls, **kwargs):
        """
        Creates a new participant with randomly generated patient_id and password.
        """

        # Ensure that a unique patient_id is generated. If it is not after
        # twenty tries, raise an error.
        patient_id = generate_easy_alphanumeric_string()
        for _ in range(20):
            if not cls.objects.filter(patient_id=patient_id).exists():
                # If patient_id does not exist in the database already
                break
            patient_id = generate_easy_alphanumeric_string()
        else:
            raise RuntimeError('Could not generate unique Patient ID for new Participant.')

        # Create a Participant, and generate for them a password
        participant = cls(patient_id=patient_id, **kwargs)
        password = participant.reset_password()  # this saves participant

        return patient_id, password

    def generate_hash_and_salt(self, password):
        return generate_user_hash_and_salt(password)

    def debug_validate_password(self, compare_me):
        """
        Checks if the input matches the instance's password hash, but does
        the hashing for you for use on the command line. This is necessary
        for manually checking that setting and validating passwords work.
        """
        compare_me = device_hash(compare_me)
        return compare_password(compare_me, self.salt, self.password)

    def set_device(self, device_id):
        self.device_id = device_id
        self.save()

    def set_os_type(self, os_type):
        self.os_type = os_type
        self.save()

    def clear_device(self):
        self.device_id = ''
        self.save()

    def __str__(self):
        return '{} {} of Study {}'.format(self.__class__.__name__, self.patient_id, self.study.name)


class ParticipantFieldValue(models.Model):
    """
    These objects can be deleted.
    """
    participant = models.ForeignKey(Participant, on_delete=models.PROTECT, related_name='field_values')
    field = models.ForeignKey('StudyField', on_delete=models.CASCADE, related_name='field_values')
    value = models.TextField()

    class Meta:
        unique_together = (("participant", "field"),)


class Researcher(AbstractPasswordUser):
    """
    The Researcher database object contains the password hashes and unique usernames of any
    researchers, as well as their data access credentials. A Researcher can be attached to
    multiple Studies, and a Researcher may also be an admin who has extra permissions.
    A Researcher uses web, so their passwords are hashed accordingly.
    """

    username = models.CharField(max_length=32, unique=True, help_text='User-chosen username, stored in plain text')
    site_admin = models.BooleanField(default=False, help_text='Whether the researcher is also an admin')

    access_key_id = models.CharField(max_length=64, validators=[standard_base_64_validator], unique=True, null=True, blank=True)
    access_key_secret = models.CharField(max_length=44, validators=[url_safe_base_64_validator], blank=True)
    access_key_secret_salt = models.CharField(max_length=24, validators=[url_safe_base_64_validator], blank=True)

    @classmethod
    def create_with_password(cls, username, password, **kwargs):
        """
        Creates a new Researcher with provided username and password. They will initially
        not be associated with any Study.
        """

        researcher = cls(username=username, **kwargs)
        researcher.set_password(password)
        # todo add check to see if access credentials are in kwargs
        researcher.reset_access_credentials()
        return researcher

    @classmethod
    def create_without_password(cls, username):
        """
        Create a new Researcher with provided username and no password
        """

        r = cls(username=username, password='fakepassword', salt='cab', admin=False)
        r.reset_access_credentials()
        return r

    @classmethod
    def check_password(cls, username, compare_me):
        """
        Checks if the provided password matches the hash of the provided Researcher's password.
        """
        if not Researcher.objects.filter(username=username).exists():
            return False
        researcher = Researcher.objects.get(username=username)
        return researcher.validate_password(compare_me)

    @classmethod
    def filter_alphabetical(self, *args, **kwargs):
        """ Sort the Researchers a-z by username ignoring case, exclude special user types. """
        return (
            Researcher.objects
                .annotate(username_lower=Func(F('username'), function='LOWER'))
                .order_by('username_lower')
                .exclude(username__contains="BATCH USER").exclude(username__contains="AWS LAMBDA")
                .filter(*args, **kwargs)
        )

    @classmethod
    def get_all_researchers_by_username(cls):
        """ Gen the un-deleted Researchers a-z by username, ignoring case."""
        return cls.filter_alphabetical(deleted=False)

    def get_administered_researchers(self):
        studies = self.study_relations.filter(
            relationship=ResearcherRole.study_admin).values_list("study_id", flat=True)
        researchers = StudyRelation.objects.filter(
            study_id__in=studies).values_list("researcher_id", flat=True).distinct()
        return Researcher.objects.filter(id__in=researchers)

    def get_administered_researchers_by_username(self):
        return (
            self.get_administered_researchers()
                .filter(deleted=False)
                .annotate(username_lower=Func(F('username'), function='LOWER'))
                .order_by('username_lower')
                .exclude(username__contains="BATCH USER").exclude(username__contains="AWS LAMBDA")
        )

    def get_administered_studies_by_name(self):
        from database.models import Study
        return Study._get_administered_studies_by_name(self)

    def generate_hash_and_salt(self, password):
        return generate_hash_and_salt(password)

    def elevate_to_site_admin(self):
        self.site_admin = True
        self.save()

    def elevate_to_study_admin(self, study):
        study_relation = StudyRelation.objects.get(researcher=self, study=study)
        study_relation.relationship = ResearcherRole.study_admin
        study_relation.save()

    def validate_access_credentials(self, proposed_secret_key):
        """ Returns True/False if the provided secret key is correct for this user."""
        return compare_password(
            proposed_secret_key,
            self.access_key_secret_salt,
            self.access_key_secret
        )

    def reset_access_credentials(self):
        access_key = generate_random_string()[:64]
        secret_key = generate_random_string()[:64]
        secret_hash, secret_salt = generate_hash_and_salt(secret_key)
        self.access_key_id = access_key
        self.access_key_secret = secret_hash
        self.access_key_secret_salt = secret_salt
        self.save()
        return access_key, secret_key

    def get_admin_study_relations(self):
        return self.study_relations.filter(relationship=ResearcherRole.study_admin)

    def get_researcher_study_relations(self):
        return self.study_relations.filter(relationship=ResearcherRole.researcher)

    def get_researcher_studies_by_name(self):
        from database.models import Study
        return Study.get_researcher_studies_by_name(self)

    def get_visible_studies_by_name(self):
        if self.site_admin:
            from database.models import Study
            return Study.get_all_studies_by_name()
        else:
            return self.get_researcher_studies_by_name()

    def is_study_admin(self):
        return self.get_admin_study_relations().exists()

    def check_study_admin(self, study_id):
        return self.study_relations.filter(
            relationship=ResearcherRole.study_admin,
            study_id=study_id,
        ).exists()

class StudyRelation(AbstractModel):
    """
    This is the through-model for defining the relationship between a researcher and a study.
    There are these relatioships:
        site admin
        study admin
        researcher
    """
    study = models.ForeignKey(
        'Study', on_delete=models.PROTECT, related_name='study_relations', null=False, db_index=True
    )
    researcher = models.ForeignKey(
        'Researcher', on_delete=models.PROTECT, related_name='study_relations', null=False, db_index=True
    )
    relationship = models.CharField(max_length=32, null=False, blank=False, db_index=True)

    class Meta:
        unique_together = ["study", "researcher"]

    def __str__(self):
        return "%s is a %s in %s" % (self.researcher.username,
                                     self.relationship.replace("_", " ").title(),
                                     self.study.name)

