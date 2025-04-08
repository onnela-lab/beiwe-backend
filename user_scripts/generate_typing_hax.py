# to generate the below list run this little script.  Don't use * imports.
from typing import Union
from django.db.models import Manager, QuerySet
from django.db.models.base import ModelBase
from database import models as database_models
from database.common_models import TimestampedModel, UtilityModel

from django.db.models.fields.reverse_related import OneToOneRel, ManyToOneRel

from database.survey_models import Survey
from datetime import date
# relationships_separater = "\n#=========================================================================" * 2

querysets = set()
managers = set()
# relationships = []

for name, database_model in vars(database_models).items():
    if (
        isinstance(database_model, ModelBase) and database_model._meta.abstract == False
    ):
        # the queryset and the the relashionship
        querysets.add(f"{name}QuerySet = QuerySet[{name}]")
        
        # (just adding some ~fake types here for syntax)
        database_model: Survey
        field_relationship: Union[OneToOneRel, ManyToOneRel]
        
        code_additions = []
        for field_relationship in database_model._meta.related_objects:
            # we only want the named relations
            if field_relationship.related_name is None:
                continue
            
            # setup basics
            related_model_name = field_relationship.related_model.__name__
            related_manager_typing = f"Manager[{related_model_name}]"
            # manager setup
            managers.add(f"{related_model_name}Manager = " + related_manager_typing)
        
        # this concept does not work because it usually ends up in circular import errors.
        #   The "from __future__ import annotations" only applies in-file?
        #     # setup the #property code to be added to our database models
        #     relation_code = f"@property\ndef {field_relationship.related_name}(self) -> "
        #     if isinstance(field_relationship, ManyToOneRel):
        #         relation_code = relation_code + f"{related_manager_typing}: pass"
        #     elif isinstance(field_relationship, OneToOneRel):
        #         relation_code = relation_code + f"{related_manager_typing}: pass"
        #     else:
        #         # explode if we don't have ManyToOneRel or OneToOneRel
        #         raise TypeError(f"unknown relation type: {field_relationship.__class__.__name__}")
        #     code_additions.append(relation_code)
        
        # if code_additions:
        #     code_additions.sort()
        #     if not relationships:
        #         relationships.append(relationships_separater)
        #     relationships.append(f"\nadd to {name} in {database_model.__module__}:\n")
        #     relationships.append("from __future__ import annotations")
        #     relationships.append("from django.db.models import Manager")
        #     relationships.append("from typing import List, Union")
        #     relationships.append(f"# IDE typing hax. Generated with scripts/generate_typing_hax.py on {date.today()}")
        #     relationships.extend(code_additions)
        #     relationships.append(relationships_separater)

querysets = sorted(list(querysets))
managers = sorted(list(managers))

print("# Insert into libs.internal_types. Note that these cannot be accessed inside model files themselves due to circular import issues.\n")
print(f"# Generated with scripts/generate_typing_hax.py on {date.today()}")
print("\n".join(querysets))
print()
print("\n".join(managers))
# print()
# print()
# print("\n".join(relationships))
