
import django.db.models.deletion
from django.db import migrations, models
from django.db.migrations.state import StateApps

from typing import TYPE_CHECKING



if TYPE_CHECKING:
    from database.models import DataAccessRecord as _DataAccessRecord
    from database.models import Researcher as _Researcher

def do_a_thing(apps: StateApps, schema_editor):
    # the typing isn't actually a good idea because it isn't a UtilityModel
    Researcher: _Researcher = apps.get_model('database', 'Researcher')
    DataAccessRecord: _DataAccessRecord = apps.get_model('database', 'DataAccessRecord')
    
    # bad, only do this for tiny tables
    # populate username on DataAccessRecords using the researcher's username
    for researcher in Researcher.objects.all():
        researcher.data_access_record.update(username=researcher.username)
    
    # Better:
    # bulk_update using a lookup dict to fill some value
    username_dict = dict(Researcher.objects.values_list("id", "username"))
    updates = []
    for record_id, researcher_id in DataAccessRecord.objects.values_list("id", "researcher_id"):
        username = username_dict[researcher_id]
        # if you have a warning about not being able to call DataAccessRecord here, it's wrong.
        updates.append(DataAccessRecord(pk=record_id, username=username))  # type: ignore
        # but inline the lookup, DataAccessRecord(pk=record_id, username=username_dict[researcher_id])
    
    DataAccessRecord.objects.bulk_update(updates, ["username"])

things = [
    
    migrations.RunPython(do_a_thing, migrations.RunPython.noop),
    
]




raise NotImplementedError("This is an example migration file only. Do not run it.")