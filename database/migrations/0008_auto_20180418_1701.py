# -*- coding: utf-8 -*-
# Generated by Django 1.11.5 on 2018-04-18 17:01
from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0007_auto_20180413_2033'),
    ]

    operations = [
        migrations.AlterField(
            model_name='chunkregistry',
            name='data_type',
            field=models.CharField(choices=[(b'accelerometer', b'accelerometer'), (b'bluetooth', b'bluetooth'), (b'calls', b'calls'), (b'gps', b'gps'), (b'identifiers', b'identifiers'), (b'app_log', b'app_log'), (b'power_state', b'power_state'), (b'survey_answers', b'survey_answers'), (b'survey_timings', b'survey_timings'), (b'texts', b'texts'), (b'audio_recordings', b'audio_recordings'), (b'wifi', b'wifi'), (b'proximity', b'proximity'), (b'gyro', b'gyro'), (b'magnetometer', b'magnetometer'), (b'devicemotion', b'devicemotion'), (b'reachability', b'reachability'), (b'ios_log', b'ios_log'), (b'image_survey', b'image_survey')], max_length=32),
        ),
        migrations.AlterField(
            model_name='surveyarchive',
            name='archive_end',
            field=models.DateTimeField(default=django.utils.timezone.now),
        ),
    ]
