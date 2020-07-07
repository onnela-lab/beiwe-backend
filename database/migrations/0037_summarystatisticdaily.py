# Generated by Django 2.2.11 on 2020-07-07 15:45

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0036_create_apikey'),
    ]

    operations = [
        migrations.CreateModel(
            name='SummaryStatisticDaily',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('deleted', models.BooleanField(default=False)),
                ('created_on', models.DateTimeField(auto_now_add=True)),
                ('last_updated', models.DateTimeField(auto_now=True)),
                ('date', models.DateField()),
                ('distance_diameter', models.IntegerField()),
                ('distance_from_home', models.IntegerField()),
                ('distance_travelled', models.IntegerField()),
                ('flight_distance_average', models.IntegerField()),
                ('flight_distance_standard_deviation', models.IntegerField()),
                ('flight_duration_average', models.IntegerField()),
                ('flight_duration_standard_deviation', models.IntegerField()),
                ('gps_data_missing_duration', models.IntegerField()),
                ('home_duration', models.IntegerField()),
                ('physical_circadian_rhythm', models.FloatField()),
                ('physical_circadian_rhythm_stratified', models.FloatField()),
                ('radius_of_gyration', models.IntegerField()),
                ('significant_location_count', models.IntegerField()),
                ('significant_location_entroy', models.IntegerField()),
                ('stationary_fraction', models.TextField()),
                ('text_incoming_count', models.IntegerField()),
                ('text_incoming_degree', models.IntegerField()),
                ('text_incoming_length', models.IntegerField()),
                ('text_incoming_responsiveness', models.IntegerField()),
                ('text_outgoing_count', models.IntegerField()),
                ('text_outgoing_degree', models.IntegerField()),
                ('text_outgoing_length', models.IntegerField()),
                ('text_reciprocity', models.IntegerField()),
                ('call_incoming_count', models.IntegerField()),
                ('call_incoming_degree', models.IntegerField()),
                ('call_incoming_duration', models.IntegerField()),
                ('call_incoming_responsiveness', models.IntegerField()),
                ('call_outgoing_count', models.IntegerField()),
                ('call_outgoing_degree', models.IntegerField()),
                ('call_outgoing_duration', models.IntegerField()),
                ('acceleration_direction', models.TextField()),
                ('accelerometer_coverage_fraction', models.TextField()),
                ('accelerometer_signal_variability', models.TextField()),
                ('accelerometer_univariate_summaries', models.FloatField()),
                ('device_proximity', models.BooleanField()),
                ('total_power_events', models.IntegerField()),
                ('total_screen_events', models.IntegerField()),
                ('total_unlock_events', models.IntegerField()),
                ('awake_onset_time', models.DateTimeField()),
                ('sleep_duration', models.IntegerField()),
                ('sleep_onset_time', models.DateTimeField()),
                ('participant', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='database.Participant')),
                ('study', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='database.Study')),
            ],
            options={
                'abstract': False,
            },
        ),
    ]
