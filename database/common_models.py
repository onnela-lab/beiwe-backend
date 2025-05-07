# trunk-ignore-all(ruff/E701)
from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Sequence
from datetime import date, datetime
from pprint import pprint
from random import choice as random_choice
from typing import Any, Self

from django.db import models
from django.db.models import Q, QuerySet
from django.db.models.fields import NOT_PROVIDED
from django.db.models.fields.related import RelatedField
from django.db.models.manager import BaseManager
from django.db.models.query import BaseIterable, QuerySet, ValuesIterable, ValuesListIterable
from django.utils.timezone import localtime

from constants.common_constants import DEV_TIME_FORMAT3, DT_24HR_W_TZ_W_SEC_N_PAREN, EASTERN
from constants.security_constants import OBJECT_ID_ALLOWED_CHARS


class ObjectIdError(Exception): pass


def Q_from_params(q: dict | tuple) -> Q:
    """ Convert a dict or tuple to a Q object. """
    if not isinstance(q, dict):
        return Q(*dict(q))
    return Q(**q)


def generate_objectid_string() -> str:
    return ''.join(random_choice(OBJECT_ID_ALLOWED_CHARS) for _ in range(24))


class ObjectIDModel(models.Model):
    """ Provides logic for generating unique objectid strings for a field. """
    
    @classmethod
    def generate_objectid_string(cls, field_name: str) -> str:
        """ Takes a django database class and a field name, generates a unique BSON-ObjectId-like
        string for that field.
        In order to preserve functionality throughout the codebase we need to generate a random
        string of exactly 24 characters.  The value must be typeable, and special characters should
        be avoided. """
        for _ in range(10):
            object_id = generate_objectid_string()
            if not cls.objects.filter(**{field_name: object_id}).exists():
                break
        else:
            raise ObjectIdError("Could not generate unique id for %s." % cls.__name__)
        return object_id
    
    class Meta:
        abstract = True


class JSONTextField(models.TextField):
    """ A TextField for holding JSON-serialized data. This is only different from models.TextField
    in UtilityModel.as_native_json, in that this is not JSON serialized an additional time. """


class UtilityModel(models.Model):
    """ Provides numerous utility functions and enhancements.
        All Models should subclass UtilityModel. """
    
    id: int  # this attribute is not correctly populated as an integer type in some IDEs
    
    @classmethod
    def m(cls):
        """ Prints the methods actually defined on the most-sub model subclass. """
        # "method" and "function" types, plus classmethod - UtilityModel required
        function_types = type(UtilityModel.m), type(UtilityModel.as_dict), classmethod
        instance_methods = []
        class_methods = []
        
        for attrname, attr in vars(cls).items():
            if attrname.startswith("_") or not isinstance(attr, function_types):
                continue
            
            typename = type(attr).__name__
            if typename == "function":
                instance_methods.append(attrname)
            elif typename == "classmethod":
                class_methods.append(attrname)
        
        if instance_methods:
            print(f"Instance methods:")
            for method in instance_methods:
                print(f"\t{method}")
        
        if class_methods:
            print("Class methods:")
            for method in class_methods:
                print(f"\t{method}")
    
    ######################################## Query Shortcuts #######################################
    
    @classmethod
    def value_get(cls, field_name: str, **filters) -> Any:
        return cls.flat(field_name, **filters).get()
    
    @classmethod
    def obj_get(cls, *args, **kwargs) -> Self:
        return cls.objects.get(*args, **kwargs)  # type: ignore[no-any-return] # (mypy is wrong, typeshed is correct, classmethod problem?)
    
    @classmethod
    def vlist(cls, *args, **kwargs) -> QuerySet[Any]:
        return cls.objects.values_list(*args, **{"flat": True, **kwargs} if len(args) == 1 else kwargs)
    
    @classmethod
    def vdict(cls, *args, **kwargs) -> QuerySet[dict]:
        return cls.objects.values(*args, **kwargs)
    
    @classmethod
    def fltr(cls, *args, **kwargs) -> QuerySet[Self]:
        return cls.objects.filter(*args, **kwargs)
    
    @classmethod
    def xcld(cls, *args, **kwargs) -> QuerySet[Self]:
        return cls.objects.exclude(*args, **kwargs)
    
    @classmethod
    def flat(cls, field_name: str, **filter_kwargs) -> QuerySet[Any]:
        return cls.objects.filter(**filter_kwargs).values_list(field_name, flat=True)
    
    ################################## Show nice information #######################################
    
    @classmethod
    def nice_count(cls):
        t1 = datetime.now()
        print(f"{count:= cls.objects.count():,}")
        t2 = datetime.now()
        print("this query took", (t2 - t1).total_seconds(), "seconds.")
        return count
    
    @property
    def pprint(self):
        """ shortcut for very common cli usage. """
        d = self._pprint()
        pprint(d)
        return lambda: None # so that you can call it accidentially without a crash
    
    def _pprint(self) -> dict[str, Any]:
        """ Provides a dictionary representation of the object, with some special formatting. """
        d = self.as_dict()
        for k, v in d.items():
            if isinstance(v, datetime):
                d[k] = localtime(v, EASTERN).strftime(DEV_TIME_FORMAT3)
            elif isinstance(v, date):
                d[k] = v.isoformat()
        return d
    
    @classmethod
    def summary(cls):
        for field in sorted(cls._meta.fields, key=lambda x: x.name):
            if field.is_relation:
                print(f"{field.name} - {type(field).__name__} - {field.related_model.__name__}")
            else:
                print(f"{field.name} - {type(field).__name__}")
            info = []
            if field.blank:
                info.append("blank")
            if field.null:
                info.append("null")
            if field.db_index:
                info.append("db_index")
            if field.default != NOT_PROVIDED:
                info.append(f'default: {field.default}')
            if info:
                print("\t", ", ".join(info), "\n", sep="")
            else:
                print()
    
    ###################################### Basic Serialization #####################################
    
    def as_dict(self) -> dict[str, Any]:
        """ Provides a dictionary representation of the object """
        return {field.name: getattr(self, field.name) for field in self._meta.fields}
    
    @property
    def _contents(self):
        """ Convenience purely because this is the syntax used on some other projects """
        return self.as_dict()
    
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
    
    def as_unpacked_native_python(self, field_names: tuple[str]) -> dict[str, Any]:
        """ This function returns a dictionary of the desired fields, unpacking any JSONTextField.
        DO NOT MAKE A VERSION OF THIS THAT TRIVIALLY RETURNS THE ENTIRE MODEL'S DATA. We had that
        and it caused numerous bugs, security issues, and wasted time. If you want to do that use
        the local_field_names() methods to get the field names.
        Will raise value errors if you pass in invalid field names."""
        
        # check if the field names are valid
        real_field_names = self.__class__.local_field_names()  # data structure optimization?
        for field_name in field_names:
            if field_name not in real_field_names:
                raise ValueError(f"Field name {field_name} is not a valid field name for this model.")
        
        ret = {}
        for field in self._meta.fields:
            if field.name not in field_names:
                continue
            elif isinstance(field, JSONTextField):
                # If the field is a JSONTextField, load the field's value before returning
                ret[field.name] = json.loads(getattr(self, field.name))
            else:
                # Otherwise, just return the field's value directly
                ret[field.name] = getattr(self, field.name)
        
        return ret
    
    @classmethod
    def local_field_names(cls) -> list[str]:
        """ helper for mostly these basic serialization methods, but useful elswhere. """
        return [f.name for f in cls._meta.fields if not isinstance(f, RelatedField)]
    
    #################################### Mutation Methods ##########################################
    
    def save(self, *args, **kwargs):
        # Raise a ValidationError if any data is invalid
        self.full_clean()
        super().save(*args, **kwargs)
    
    def update(self, **kwargs):
        """ Convenience method on to update the database with a dictionary or kwargs."""
        for attr, value in kwargs.items():
            if not hasattr(self, attr):
                # This safety is good enough, only fails when using defer.
                raise Exception(f"unexpected parameter: {attr}")
            setattr(self, attr, value)
        self.save()
    
    def update_only(self, **kwargs):
        """ As update, but only saves the fields provided. (its extremely concise) """
        if "last_updated" in kwargs and hasattr(self, "last_updated"):
            raise ValueError("last_updated cannot be updated directly.")
        
        for attr, value in kwargs.items():
            if not hasattr(self, attr):
                raise Exception(f"unexpected parameter: {attr}")
            setattr(self, attr, value)
        self.save(update_fields=kwargs.keys())
    
    def force_update_only(self, **kwargs):
        """ As update_only, but uses alternate mechanism that does not check for last_updated and
        will pull the rest of the data from the database. 2 DB queries required. """
        for attr, value in kwargs.items():
            if not hasattr(self, attr):
                raise Exception(f"unexpected parameter: {attr}")
        self.__class__.objects.filter(pk=self.pk).update(**kwargs)
        self.refresh_from_db()
    
    ################################ Convenience Dict Lookups ######################################
    
    #TODO: make filters keywords, validate inputs by type for misuse
    @classmethod
    def make_lookup_dict(cls, keys: Sequence[str], values: Sequence[str], **filters) -> dict:
        """ Given a base model, a list of key fields, and a list of value fields, this function will
        return a dictionary that maps the key fields to the value fields. """
        return make_lookup_dict(cls.objects, keys, values, **filters)
    
    @classmethod
    def make_lookup_dict_list(cls, keys: Sequence[str], values: Sequence[str], **filters) -> dict:
        """ Given a base model, a list of key fields, and a list of value fields, this function will
        return a dictionary that maps the key fields to any number of value fields as lists. """
        return make_lookup_dict_list(cls.objects, keys, values, **filters)
    
    ####################################### __str__! ###############################################
    
    def __str__(self) -> str:
        """ multipurpose object representation """
        if hasattr(self, 'study') and self.study and hasattr(self, 'name') and self.name:
            return f'{self.__class__.__name__} {self.pk} "{self.name}" of Study {self.study.name}'
        elif hasattr(self, 'study') and self.study:
            return f'{self.__class__.__name__} {self.pk} of Study {self.study.name}'
        elif hasattr(self, 'name') and self.name:
            return f'{self.__class__.__name__} {self.name}'
        else:
            return f'{self.__class__.__name__} {self.pk}'
    
    class Meta:
        abstract = True


class CreatedOnModel(UtilityModel):
    """ CreatedOnModels record their creation time. """
    created_on = models.DateTimeField(auto_now_add=True)
    class Meta:
        abstract = True


class TimestampedModel(CreatedOnModel):
    """ TimestampedModels record their creation time and last updated time (if they use .save()). """
    last_updated = models.DateTimeField(auto_now=True)
    class Meta:
        abstract = True


#################################### Lookup Dictionaries ###########################################


def make_lookup_dict(queryable, keys: Sequence[str], values: Sequence[str], **filters) -> dict:
    """ Given quuery, filters, a list of key fields, and a list of value fields, this function will
    return a dictionary that maps the key fields to the value fields. """
    keys, values, queryable, k_len, v_len, dd = lookup_dict_setup(queryable, keys, values, **filters)
    
    # optimization paths for single keys, these are effectively of the form
    # for all_fld_vals in query:
    #     key = all_fld_vals[:key_count][0]
    #     value = all_fld_vals[key_count:][0]
    #     dd[key] = (value)
    if k_len == 1 and v_len == 1:
        return dict(queryable)
    elif k_len == 1:
        for all_fld_vals in queryable: dd[all_fld_vals[0]] = all_fld_vals[1:]
    elif v_len == 1:
        for all_fld_vals in queryable: dd[all_fld_vals[:k_len]] = all_fld_vals[-1]
    else:
        for all_fld_vals in queryable: dd[all_fld_vals[:k_len]] = all_fld_vals[k_len:]
    return dict(dd)


def make_lookup_dict_list(queryable, keys: Sequence[str], values: Sequence[str], **filters) -> dict:
    """ Given quuery, filters, a list of key fields, and a list of value fields, this function will
    return a dictionary that maps the key fields to any number of value fields as lists. """
    keys, values, queryable, k_len, v_len, dd = lookup_dict_setup(queryable, keys, values, **filters)
    
    # as in make_lookup_dict, but with a defaultdict(list) and append instead of assignment.
    if k_len == 1 and v_len == 1:
        for all_fld_vals in queryable: dd[all_fld_vals[0]].append(all_fld_vals[1])
    elif k_len == 1:
        for all_fld_vals in queryable: dd[all_fld_vals[0]].append(all_fld_vals[1:])
    elif v_len == 1:
        for all_fld_vals in queryable: dd[all_fld_vals[:k_len]].append(all_fld_vals[-1])
    else:
        for all_fld_vals in queryable: dd[all_fld_vals[:k_len]].append(all_fld_vals[k_len:])
    return dict(dd)


def lookup_dict_setup(queryable, keys: Sequence[str], values: Sequence[str], **filters):
    # common parsing of the parameters passed to either of the lookup_dict functions
    # make single strings more convenient
    keys = [keys] if isinstance(keys, str) else keys
    values = [values] if isinstance(values, str) else values
    #TODO: validate that all keywords are .... strings maybe?
    ret_query = queryable.filter(**filters).values_list(*keys, *values)
    keys_count = len(keys)
    values_count = len(values)
    dd = defaultdict(list)
    return keys, values, ret_query, keys_count, values_count, dd


"""
Some probably ill-advised hacks

The Below is a bunch of monkeypatches that add some functionality to the django queryset-adjacent
classes.  

And then you can just do:
    study.interventions.vlist("name", "study_object_id")
And it just works.

I'm sure in some way it doesn't work, like the IDE doesn't understand it and I couldn't get the full
type hinting working.
"""

_special_format = "<" + DT_24HR_W_TZ_W_SEC_N_PAREN.replace(" ", "_") + ">"

# Monkeypatches that add a ~pprint property to django querysets for date/time objects.
def terminal_legible_dt_as_a_method(self: BaseIterable):
    end = ""
    ret = [terminal_legible_dt(v) for v in list(self[:31])]
    if len(ret) > 30:
        ret.pop(30)  # there we go, if its too long we REPLACE a "..." to the end.....
        end = ", '...(remaining elements truncated)...'"
    
    return f'<QuerySet [{", ".join([(repr(x)) for x in ret])}{end}]>'

# part 2
def terminal_legible_dt(x: Any) -> Any:
    if isinstance(x, datetime):
        return localtime(x, EASTERN).strftime(_special_format)
    elif isinstance(x, date):
        return x.isoformat()
    
    # .values()
    elif isinstance(x, dict) or issubclass(type(x), dict):
        new = {}
        for k, v in x.items():
            new[k] = terminal_legible_dt(v)
        return new
    
    # .values_list(flat!=True) (and many dates)
    elif isinstance(x, tuple|list) or issubclass(type(x), tuple|list):
        return tuple(terminal_legible_dt(v) for v in x)
    
    return x


# instance methods that we monkeypatch onto the ~queryset classes
# These all function as the UtilityModel methods (but don't contain `.objects` in their code).
def _value_get(self, field_name: str, **filters) -> Any:
    return self.flat(field_name, **filters).get()


def _obj_get(self, *args, **kwargs) -> Self:  # type: ignore
    return self.get(*args, **kwargs)


def _vlist(self, *args, **kwargs) -> BWValuesListIterable[Any]:  # type: ignore
    return self.values_list(*args, **{"flat": True, **kwargs} if len(args) == 1 else kwargs)


def _vdict(self, *args, **kwargs) -> QuerySet[dict[str, Any]]:  # type: ignore
    return self.values(*args, **kwargs)


def _fltr(self, *args, **kwargs) -> QuerySet[Self]:  # type: ignore
    return self.filter(*args, **kwargs)


def _xcld(self, *args, **kwargs) -> QuerySet[Self]:  # type: ignore
    return self.exclude(*args, **kwargs)


def _flat(self, field_name: str, **filter_kwargs) -> QuerySet[Any]:
    return self.filter(**filter_kwargs).values_list(field_name, flat=True)

# and this is where we assign them - there's a bunch of typing errors, they are wrong. XD
for _T in (QuerySet, BaseIterable, BaseManager, ValuesIterable, ValuesListIterable):
    _T.vlist = _vlist  # type: ignore
    _T.value_get = _value_get  # type: ignore
    _T.vdict = _vdict  # type: ignore
    _T.obj_get = _obj_get  # type: ignore
    _T.fltr = _fltr  # type: ignore
    _T.xcld = _xcld  # type: ignore
    _T.flat = _flat  # type: ignore
    _T.__repr__ = terminal_legible_dt_as_a_method  # type: ignore
