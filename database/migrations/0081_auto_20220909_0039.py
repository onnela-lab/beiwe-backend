# Generated by Django 3.2.15 on 2022-09-09 00:39

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0080_summarystatisticdaily_timezone'),
    ]

    operations = [
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='beiwe_accelerometer_bytes',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='beiwe_ambient_audio_bytes',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='beiwe_app_log_bytes',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='beiwe_audio_recordings_bytes',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='beiwe_bluetooth_bytes',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='beiwe_calls_bytes',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='beiwe_devicemotion_bytes',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='beiwe_gps_bytes',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='beiwe_gyro_bytes',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='beiwe_identifiers_bytes',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='beiwe_image_survey_bytes',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='beiwe_ios_log_bytes',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='beiwe_magnetometer_bytes',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='beiwe_power_state_bytes',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='beiwe_proximity_bytes',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='beiwe_reachability_bytes',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='beiwe_survey_answers_bytes',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='beiwe_survey_timings_bytes',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='beiwe_texts_bytes',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='beiwe_wifi_bytes',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
    ]
