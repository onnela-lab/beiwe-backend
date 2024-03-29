# Generated by Django 3.2.24 on 2024-02-28 09:04

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0113_appheartbeats_message'),
    ]

    operations = [
        migrations.CreateModel(
            name='DeviceStatusReportHistory',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('app_os', models.CharField(max_length=32)),
                ('os_version', models.CharField(max_length=32)),
                ('app_version', models.CharField(max_length=32)),
                ('endpoint', models.TextField()),
                ('compressed_report', models.BinaryField()),
                ('participant', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name='device_status_reports', to='database.participant')),
            ],
            options={
                'abstract': False,
            },
        ),
    ]
