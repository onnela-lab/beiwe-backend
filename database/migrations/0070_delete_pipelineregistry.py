# Generated by Django 2.2.25 on 2022-03-03 21:01

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0069_filetoprocess_os_type'),
    ]

    operations = [
        migrations.DeleteModel(
            name='PipelineRegistry',
        ),
    ]
