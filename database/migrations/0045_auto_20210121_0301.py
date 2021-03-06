# Generated by Django 2.2.14 on 2021-01-21 03:01

import timezone_field.fields
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0044_auto_20210115_2300'),
    ]

    operations = [
        migrations.AddField(
            model_name='participant',
            name='push_notification_unreachable',
            field=models.SmallIntegerField(default=True),
        ),
        migrations.AddField(
            model_name='participant',
            name='timezone',
            field=timezone_field.fields.TimeZoneField(default='America/New_York'),
        ),
    ]
