# Generated by Django 3.2.13 on 2022-05-01 00:50

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0076_auto_20220420_2045'),
    ]

    operations = [
        migrations.DeleteModel(
            name='FileProcessLock',
        ),
    ]
