# Generated by Django 3.2.13 on 2022-06-22 03:16

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0079_delete_decryptionkeyerror'),
    ]

    operations = [
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='timezone',
            field=models.CharField(default='UTC', max_length=10),
            preserve_default=False,
        ),
    ]
