# Generated by Django 4.2.11 on 2024-06-17 16:40

from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0125_participant_first_register_user'),
    ]

    operations = [
        migrations.AddField(
            model_name='devicestatusreporthistory',
            name='created_on',
            field=models.DateTimeField(default=django.utils.timezone.now),
        ),
    ]
