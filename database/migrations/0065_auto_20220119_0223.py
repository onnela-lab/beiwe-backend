# Generated by Django 2.2.25 on 2022-01-19 02:23

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0064_cleanup_migration'),
    ]

    operations = [
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='willow_incoming_call_duration',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='summarystatisticdaily',
            name='willow_outgoing_call_duration',
            field=models.FloatField(blank=True, null=True),
        ),
    ]
