# Generated by Django 3.2.15 on 2022-10-06 15:56

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0085_auto_20221005_2203'),
    ]

    operations = [
        migrations.AlterField(
            model_name='scheduledevent',
            name='most_recent_event',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.DO_NOTHING, to='database.archivedevent'),
        ),
    ]
