# Generated by Django 3.2.20 on 2023-09-05 18:16

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0105_participant_unknown_timezone'),
    ]

    operations = [
        migrations.AddField(
            model_name='researcher',
            name='most_recent_page',
            field=models.TextField(blank=True, null=True),
        ),
    ]
