# This file needs to populate all the other models in order for django to identify that it has
# all the models

from .common_models import *                # noqa
from .study_models import *                 # noqa
from .survey_models import *                # noqa
from .user_models_common import *           # noqa
from .user_models_participant import *      # noqa
from .user_models_researcher import *       # noqa
from .profiling_models import *             # noqa
from .data_access_models import *           # noqa
from .dashboard_models import *             # noqa
from .schedule_models import *              # noqa
from .system_models import *                # noqa
from .forest_models import *                # noqa
from .security_models import *              # noqa


from django.core.validators import ProhibitNullCharactersValidator
from django.db.models.sql.query import Query


# dynamically inject the ProhibitNullCharactersValidator validator on all char and text fields.
# This takes about 1 millisecond (yuck, it changes size on iteration)
def _inject_prohibit_null_characters_validator():
    from django.db.models import fields
    from django.db.models.base import ModelBase
    
    for name, database_model in [(k, v) for k, v in vars().items()]:
        if isinstance(database_model, ModelBase):
            for field in database_model._meta.fields:
                # print(name, field, type(field))
                # checked: Binary fields are not subclasses of textfields
                if isinstance(field, (fields.CharField, fields.TextField)):
                    if ProhibitNullCharactersValidator not in field.validators:
                        field.validators.append(ProhibitNullCharactersValidator())

_inject_prohibit_null_characters_validator()

#
## monkeypatch django.db.models.sql.query.Query so it has a repr that shows the query
#

def query_repr(self: Query) -> str:
    """ This is a hack to make the QuerySet look nice in the terminal. """
    return f"```\n{self}\n```"

Query.__repr__ = query_repr
