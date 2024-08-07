# Generated by Django 3.2.16 on 2022-12-05 11:32

import django.core.validators
from django.db import migrations, models
from django.db.migrations.state import StateApps

from libs.utils.security_utils import to_django_password_components


def apply_password_transform(apps: StateApps, schema_editor):
    do_researchers(apps)
    do_participants(apps)
    do_api_keys(apps)
    # raise Exception("NOPE")


def do_researchers(apps: StateApps):
    Researcher = apps.get_model('database', 'Researcher')
    for researcher in Researcher.objects.all():
        updates = {}
        if researcher.password:
            updates["password"] = to_django_password_components(
                "sha1", 1000, researcher.password.encode(), researcher.salt.encode()
            )
        if researcher.access_key_secret:
            updates["access_key_secret"] = to_django_password_components(
                "sha1", 1000, researcher.access_key_secret.encode(), researcher.access_key_secret_salt.encode()
            )
        if updates:
            Researcher.objects.filter(id=researcher.id).update(**updates)


def do_participants(apps: StateApps):
    Participant = apps.get_model('database', 'Participant')
    for participant in Participant.objects.all():
        updates = {}
        if participant.password:
            updates["password"] = to_django_password_components(
                "sha1", 1000, participant.password.encode(), participant.salt.encode()
            )
            Participant.objects.filter(id=participant.id).update(**updates)


def do_api_keys(apps: StateApps):
    ApiKey = apps.get_model('database', 'ApiKey')
    for apikey in ApiKey.objects.all():
        updates = {}
        if apikey.access_key_secret:
            updates["access_key_secret"] = to_django_password_components(
                "sha1", 1000, apikey.access_key_secret.encode(), apikey.access_key_secret_salt.encode()
            )
            ApiKey.objects.filter(id=apikey.id).update(**updates)


class Migration(migrations.Migration):
    
    dependencies = [
        ('database', '0093_dataaccessrecord'),
    ]
    
    operations = [
        migrations.AlterField(
            model_name='apikey',
            name='access_key_secret',
            field=models.CharField(blank=True, max_length=256, validators=[django.core.validators.RegexValidator('^(sha1|sha256)\\$[0-9]+\\$[0-9a-zA-Z_\\-]+={0,2}\\$[0-9a-zA-Z_\\-]+={0,2}$')]),
        ),
        migrations.AlterField(
            model_name='participant',
            name='password',
            field=models.CharField(max_length=256, validators=[django.core.validators.RegexValidator('^(sha1|sha256)\\$[0-9]+\\$[0-9a-zA-Z_\\-]+={0,2}\\$[0-9a-zA-Z_\\-]+={0,2}$')]),
        ),
        migrations.AlterField(
            model_name='researcher',
            name='access_key_secret',
            field=models.CharField(blank=True, max_length=256, validators=[django.core.validators.RegexValidator('^(sha1|sha256)\\$[0-9]+\\$[0-9a-zA-Z_\\-]+={0,2}\\$[0-9a-zA-Z_\\-]+={0,2}$')]),
        ),
        migrations.AlterField(
            model_name='researcher',
            name='password',
            field=models.CharField(max_length=256, validators=[django.core.validators.RegexValidator('^(sha1|sha256)\\$[0-9]+\\$[0-9a-zA-Z_\\-]+={0,2}\\$[0-9a-zA-Z_\\-]+={0,2}$')]),
        ),
        
        migrations.RunPython(apply_password_transform, reverse_code=migrations.RunPython.noop),
        
        migrations.RemoveField(
            model_name='apikey',
            name='access_key_secret_salt',
        ),
        migrations.RemoveField(
            model_name='participant',
            name='salt',
        ),
        migrations.RemoveField(
            model_name='researcher',
            name='access_key_secret_salt',
        ),
        migrations.RemoveField(
            model_name='researcher',
            name='salt',
        ),
    ]
