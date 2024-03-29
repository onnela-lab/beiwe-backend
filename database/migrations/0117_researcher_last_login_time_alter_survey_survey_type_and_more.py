# Generated by Django 4.2.11 on 2024-03-15 18:20

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0116_remove_summarystatisticdaily_beiwe_image_survey_bytes'),
    ]

    operations = [
        migrations.AddField(
            model_name='researcher',
            name='last_login_time',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='survey',
            name='survey_type',
            field=models.CharField(choices=[('audio_survey', 'audio_survey'), ('tracking_survey', 'tracking_survey')], help_text='What type of survey this is.', max_length=16),
        ),
        migrations.AlterField(
            model_name='surveyarchive',
            name='survey_type',
            field=models.CharField(choices=[('audio_survey', 'audio_survey'), ('tracking_survey', 'tracking_survey')], help_text='What type of survey this is.', max_length=16),
        ),
    ]
