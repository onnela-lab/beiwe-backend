# Generated by Django 4.2.15 on 2024-10-17 07:45

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0131_uuid_migration'),
    ]

    operations = [
        migrations.AddField(
            model_name='participant',
            name='last_active_survey_ids',
            field=models.TextField(blank=True, default=None, null=True),
        ),
        migrations.AddField(
            model_name='participant',
            name='raw_notification_report',
            field=models.TextField(blank=True, default=None, null=True),
        ),
    ]
