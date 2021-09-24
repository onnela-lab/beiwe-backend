# Generated by Django 2.2.24 on 2021-09-24 22:46

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0059_devicesettings_ambient_audio'),
    ]

    operations = [
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='accelerometer_bytes',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='ambient_audio_bytes',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='app_log_bytes',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='audio_recordings_bytes',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='bluetooth_bytes',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='calls_bytes',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='devicemotion_bytes',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='gps_bytes',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='gyro_bytes',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='identifiers_bytes',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='image_survey_bytes',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='ios_log_bytes',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='magnetometer_bytes',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='power_state_bytes',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='proximity_bytes',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='reachability_bytes',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='survey_answers_bytes',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='survey_timings_bytes',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='texts_bytes',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='wifi_bytes',
            field=models.IntegerField(blank=True, null=True),
        ),
    ]
