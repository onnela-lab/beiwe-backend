# Generated by Django 2.2.25 on 2022-03-23 23:22

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0071_auto_20220323_2245'),
    ]

    operations = [
        migrations.AlterField(
            model_name='surveyarchive',
            name='archive_start',
            field=models.DateTimeField(db_index=True),
        ),
    ]
