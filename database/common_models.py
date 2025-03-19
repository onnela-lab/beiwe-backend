# trunk-ignore-all(ruff/E701)
from __future__ import annotations

import builtins
import json
from collections import defaultdict
from datetime import date, datetime, tzinfo
from pprint import pprint
from random import choice as random_choice
from typing import Any, Dict, List, Sequence, Tuple, Union

import dateutil
from django.db import models
from django.db.models import Q, QuerySet
from django.db.models.fields import NOT_PROVIDED
from django.db.models.fields.related import RelatedField
from django.db.models.manager import BaseManager
from django.db.models.query import BaseIterable, QuerySet
from django.utils.timezone import localtime
from typing_extensions import Self

from constants.common_constants import (DEV_TIME_FORMAT3, DT_24HR_N_TZ_W_SEC_N_PAREN,
    DT_24HR_W_TZ_W_SEC_N_PAREN, EASTERN)
from constants.security_constants import OBJECT_ID_ALLOWED_CHARS


class ObjectIdError(Exception): pass


def Q_from_params(q: Union[Dict, Tuple]) -> Q:
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
    
    ######################################## Query Shortcuts #######################################
    
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
        print("{:,}".format(count:= cls.objects.count()))
        t2 = datetime.now()
        print("this query took", (t2 - t1).total_seconds(), "seconds.")
        return count
    
    @property
    def pprint(self):
        """ shortcut for very common cli usage. """
        d = self._pprint()
        pprint(d)
        return lambda: None # so that you can call it accidentially without a crash
    
    def _pprint(self) -> Dict[str, Any]:
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
    
    def as_dict(self) -> Dict[str, Any]:
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
    
    def as_unpacked_native_python(self, field_names: Tuple[str]) -> Dict[str, Any]:
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
    def local_field_names(cls) -> List[str]:
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
                raise Exception(f"unpexpected parameter: {attr}")
            setattr(self, attr, value)
        self.save()
    
    def update_only(self, **kwargs):
        """ As update, but only saves the fields provided. (its extremely concise) """
        if "last_updated" in kwargs and hasattr(self, "last_updated"):
            raise ValueError("last_updated cannot be updated directly.")
        
        for attr, value in kwargs.items():
            if not hasattr(self, attr):
                raise Exception(f"unpexpected parameter: {attr}")
            setattr(self, attr, value)
        self.save(update_fields=kwargs.keys())
    
    def force_update_only(self, **kwargs):
        """ As update_only, but uses alternate mechanism that does not check for last_updated and
        will pull the rest of the data from the database. 2 DB queries required. """
        for attr, value in kwargs.items():
            if not hasattr(self, attr):
                raise Exception(f"unpexpected parameter: {attr}")
        self.__class__.objects.filter(pk=self.pk).update(**kwargs)
        self.refresh_from_db()
    
    ################################ Convenience Dict Lookups ######################################
    
    #TODO: make filters keywords, validate inputs by type for misuse
    @classmethod
    def make_lookup_dict(cls, filters: Dict, keys: Sequence[str], values: Sequence[str]) -> Dict:
        """ Given a base model, a list of key fields, and a list of value fields, this function will
        return a dictionary that maps the key fields to the value fields. """
        return make_lookup_dict(cls.objects, keys, values, **filters)
    
    @classmethod
    def make_lookup_dict_list(cls, filters: Dict, keys: Sequence[str], values: Sequence[str]) -> Dict:
        """ Given a base model, a list of key fields, and a list of value fields, this function will
        return a dictionary that maps the key fields to any number of value fields as lists. """
        return make_lookup_dict_list(cls.objects, keys, values, **filters)
    
    ####################################### __str__! ###############################################
    
    def __str__(self) -> str:
        """ multipurpose object representation """
        if hasattr(self, 'study') and hasattr(self, 'name') and self.name:
            return f'{self.__class__.__name__} {self.pk} "{self.name}" of Study {self.study.name}'
        elif hasattr(self, 'study'):
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


def make_lookup_dict(queryable, keys: Sequence[str], values: Sequence[str], **filters) -> Dict:
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


def make_lookup_dict_list(queryable, keys: Sequence[str], values: Sequence[str], **filters) -> Dict:
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


#
# monkeypatches to add a nicely printable method on django querysets+....
#


_rtypes = {k for v,k in vars(builtins).items() if isinstance(k, type)}


def rprint(x: Any, nest_level=0) -> Any:
    """ Tries to construct legible nested structures version of a collection. """
    
    if nest_level >= 10:
        return f"[nesting at {nest_level}]"
    
    if isinstance(x, datetime):
        v = _tformat(x)
        if x.tzinfo is not None:  # if the orig had a timezone
            if x.tzinfo != EASTERN:
                v = v + f" (was {x.strftime("%Z")})"
        return v
    
    elif isinstance(x, date):
        return x.isoformat()
    
    elif isinstance(x, dict) or issubclass(type(x), dict):
        new = {}
        for k, v in x.items():
            new[k] = rprint(v, nest_level + 1)
        return new
    
    elif isinstance(x, list) or issubclass(type(x), list):
        return [rprint(v, nest_level + 1) for v in x]
    
    elif isinstance(x, tuple) or issubclass(type(x), tuple):
        return tuple(rprint(v, nest_level + 1) for v in x)
    
    elif isinstance(x, set) or issubclass(type(x), set):
        y = ["{this one's a set}"]
        for v in x:
            y.append(rprint(v, nest_level + 1))
        return y
    
    elif hasattr(x, "__iter__") and type(x) not in _rtypes:
        y = [f"(this one's a {type(x).__name__})"]
        for v in x:
            y.append(rprint(v, nest_level + 1))
        return y
    
    else:
        return x


def _tformat(dt: datetime, tz: tzinfo = EASTERN, fmt: str = DT_24HR_W_TZ_W_SEC_N_PAREN) -> str:
    if dt.tzinfo is None:
        return dt.strftime(DT_24HR_N_TZ_W_SEC_N_PAREN) + " (None)"
    return localtime(dt, tz).strftime(fmt)


def rprint_as_method(self: BaseIterable, nest_level=0):
    return rprint(list(self), nest_level)


# QuerySet.rprint = rprint_as_method  # not actually super useful, just gets the repr. # type: ignore  
BaseIterable.rprint = rprint_as_method  # values_list, values, etc.  # type: ignore
BaseManager.rprint = rprint_as_method  # "objects." # type: ignore

# this one is probably too dangerous, think study passwords
# def new_repr(self: BaseIterable):
#     from pprint import pformat
#     ret = list(self.values()[:4])
#     ret.append(" ... ")
#     return pformat(ret, width=os.get_terminal_size().columns)
# BaseManager.__repr__ = new_repr  # type: ignore
