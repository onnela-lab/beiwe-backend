import json
from random import choice as random_choice

from django.db import models
from django.db.models.fields.related import RelatedField
from flask import abort
from timezone_field import TimeZoneField

from config.study_constants import OBJECT_ID_ALLOWED_CHARS


class ObjectIdError(Exception): pass


class JSONTextField(models.TextField):
    """
    A TextField for holding JSON-serialized data. This is only different from models.TextField
    in UtilityModel.as_native_json, in that this is not JSON serialized an additional time.
    """


class UtilityModel(models.Model):
    """ Provides numerous utility functions and enhancements.
        All Models should subclass UtilityModel. """

    @classmethod
    def generate_objectid_string(cls, field_name):
        """
        Takes a django database class and a field name, generates a unique BSON-ObjectId-like
        string for that field.
        In order to preserve functionality throughout the codebase we need to generate a random
        string of exactly 24 characters.  The value must be typeable, and special characters
        should be avoided.
        """

        for _ in range(10):
            object_id = ''.join(random_choice(OBJECT_ID_ALLOWED_CHARS) for _ in range(24))
            if not cls.objects.filter(**{field_name: object_id}).exists():
                break
        else:
            raise ObjectIdError("Could not generate unique id for %s." % cls.__name__)

        return object_id

    @classmethod
    def get_or_404(cls, *args, **kwargs):
        try:
            return cls.objects.get(*args, **kwargs)
        except cls.DoesNotExist:
            return abort(404)

    @classmethod
    def query_set_as_unpacked_native_json(cls, query_set, remove_timestamps=True):
        return json.dumps([obj.as_unpacked_native_python(remove_timestamps) for obj in query_set])

    def as_dict(self):
        """ Provides a dictionary representation of the object """
        return {field.name: getattr(self, field.name) for field in self._meta.fields}

    @property
    def _contents(self):
        """ Convenience purely because this is the syntax used on some other projects """
        return self.as_dict()

    @property
    def _uncached_instance(self):
        """ convenience for grabbing a new, different model object. Not intended for use in production. """
        return self._meta.model.objects.get(id=self.id)

    @property
    def _related(self):
        """ Gets all related objects for this database object (warning: probably huge).
            This is intended for debugging only. """
        ret = {}
        db_calls = 0
        entities_returned = 0
        for related_field in self._meta.related_objects:
            # There is no predictable way to access related models that do not have related names.
            # ... unless there is a way to inspect related_field.related_model._meta._relation_tree
            # and determine the field relationship to then magically create a query? :D

            # one to one fields use this...
            if related_field.one_to_one and related_field.related_name:
                related_entity = getattr(self, related_field.related_name)
                ret[related_field.related_name] = related_entity.as_dict() if related_entity else None
            
            # many to one and many to many use this.
            elif related_field.related_name:
                # get all the related things using .values() for access, but convert to dict
                # because the whole point is we want these thing to be prettyprintable and nice.
                related_manager = getattr(self, related_field.related_name)
                db_calls += 1
                ret[related_field.related_name] = [x for x in related_manager.all().values()]
                entities_returned += len(ret[related_field.related_name])

        return ret

    @property
    def _everything(self):
        """ Gets _related and _contents. Will probably be huge. Debugging only. """
        ret = self._contents
        ret.update(self._related)
        return ret

    def as_unpacked_native_python(self, remove_timestamps=True) -> dict:
        """
        Collect all of the fields of the model and return their values in a python dict,
        with json fields appropriately deserialized.
        """
        field_dict = {}
        for field in self._meta.fields:
            field_name = field.name
            if isinstance(field, RelatedField):
                # Don't include related fields in the dict
                pass
            elif isinstance(field, JSONTextField):
                # If the field is a JSONTextField, load the field's value before returning
                field_raw_val = getattr(self, field_name)
                field_dict[field_name] = json.loads(field_raw_val)
            elif remove_timestamps and (field_name == "created_on" or field_name == "last_updated"):
                continue
            elif isinstance(field, TimeZoneField):
                field_dict[field_name] = str(getattr(self, field_name))
            else:
                # Otherwise, just return the field's value directly
                field_dict[field_name] = getattr(self, field_name)

        return field_dict

    def save(self, *args, **kwargs):
        # Raise a ValidationError if any data is invalid
        self.full_clean()
        super().save(*args, **kwargs)

    def update(self, **kwargs):
        """ Convenience method on database instance objects to update the database using a dictionary.
            (exists to make porting from mongodb easier) """
        for attr, value in kwargs.items():
            setattr(self, attr, value)
        self.save()

    def __str__(self):
        """ multipurpose object representation """
        if hasattr(self, 'study'):
            return f'{self.__class__.__name__} {self.pk} of Study {self.study.name}'
        elif hasattr(self, 'name'):
            return f'{self.__class__.__name__} {self.name}'
        else:
            return f'{self.__class__.__name__} {self.pk}'

    class Meta:
        abstract = True


class TimestampedModel(UtilityModel):
    """ TimestampedModels record last access and creation time. """
    created_on = models.DateTimeField(auto_now_add=True)
    last_updated = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True
