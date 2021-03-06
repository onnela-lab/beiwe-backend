# Generated by Django 2.2.19 on 2021-05-24 21:28

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0054_auto_20210520_1931'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='summarystatisticdaily',
            name='jasmine_tracker',
        ),
        migrations.RemoveField(
            model_name='summarystatisticdaily',
            name='willow_tracker',
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='jasmine_task',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, related_name='jasmine_summary_statistics', to='database.ForestParam'),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='willow_task',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, related_name='willow_summary_statistics', to='database.ForestParam'),
        ),
    ]
