# Generated by Django 4.2.15 on 2024-12-02 17:36

import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0132_participant_last_known_surveys_available_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='devicesettings',
            name='omniring',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='devicesettings',
            name='omniring_off_duration_seconds',
            field=models.PositiveIntegerField(default=600, validators=[django.core.validators.MinValueValidator(1)]),
        ),
        migrations.AddField(
            model_name='devicesettings',
            name='omniring_on_duration_seconds',
            field=models.PositiveIntegerField(default=60, validators=[django.core.validators.MinValueValidator(1)]),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='beiwe_omniring_bytes',
            field=models.PositiveBigIntegerField(blank=True, null=True),
        ),
    ]
