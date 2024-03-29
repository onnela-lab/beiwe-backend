# Generated by Django 2.2.24 on 2021-12-02 22:34

from django.db import migrations, models


class Migration(migrations.Migration):
    
    dependencies = [
        ('database', '0061_historical_data_qty_stats'),
    ]
    
    operations = [
        # Beiwe field renames
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='accelerometer_bytes',
            new_name='beiwe_accelerometer_bytes',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='ambient_audio_bytes',
            new_name='beiwe_ambient_audio_bytes',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='app_log_bytes',
            new_name='beiwe_app_log_bytes',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='bluetooth_bytes',
            new_name='beiwe_bluetooth_bytes',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='calls_bytes',
            new_name='beiwe_calls_bytes',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='devicemotion_bytes',
            new_name='beiwe_devicemotion_bytes',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='gps_bytes',
            new_name='beiwe_gps_bytes',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='gyro_bytes',
            new_name='beiwe_gyro_bytes',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='identifiers_bytes',
            new_name='beiwe_identifiers_bytes',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='image_survey_bytes',
            new_name='beiwe_image_survey_bytes',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='ios_log_bytes',
            new_name='beiwe_ios_log_bytes',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='magnetometer_bytes',
            new_name='beiwe_magnetometer_bytes',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='power_state_bytes',
            new_name='beiwe_power_state_bytes',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='proximity_bytes',
            new_name='beiwe_proximity_bytes',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='reachability_bytes',
            new_name='beiwe_reachability_bytes',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='survey_answers_bytes',
            new_name='beiwe_survey_answers_bytes',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='survey_timings_bytes',
            new_name='beiwe_survey_timings_bytes',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='texts_bytes',
            new_name='beiwe_texts_bytes',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='audio_recordings_bytes',
            new_name='beiwe_audio_recordings_bytes',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='wifi_bytes',
            new_name='beiwe_wifi_bytes',
        ),
        
        # Willow
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='text_incoming_count',
            new_name="willow_incoming_text_count",
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='text_incoming_degree',
            new_name="willow_incoming_text_degree",
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='text_incoming_length',
            new_name="willow_incoming_text_length",
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='text_outgoing_count',
            new_name="willow_outgoing_text_count",
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='text_outgoing_degree',
            new_name="willow_outgoing_text_degree",
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='text_outgoing_length',
            new_name="willow_outgoing_text_length",
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='text_reciprocity',
            new_name="willow_incoming_text_reciprocity",
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='call_incoming_count',
            new_name="willow_incoming_call_count",
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='call_incoming_degree',
            new_name="willow_incoming_call_degree",
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='call_incoming_duration',
            new_name="willow_incoming_call_duration",
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='call_outgoing_count',
            new_name="willow_outgoing_call_count",
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='call_outgoing_degree',
            new_name="willow_outgoing_call_degree",
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='call_outgoing_duration',
            new_name="willow_outgoing_call_duration",
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='willow_incoming_MMS_count',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='willow_missed_call_count',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='willow_missed_callers',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='willow_outgoing_MMS_count',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='willow_outgoing_text_reciprocity',
            field=models.IntegerField(blank=True, null=True),
        ),
        
        # Jasmine
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='distance_diameter',
            new_name='jasmine_distance_diameter',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='distance_from_home',
            new_name='jasmine_distance_from_home',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='distance_traveled',
            new_name='jasmine_distance_traveled',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='flight_distance_average',
            new_name='jasmine_flight_distance_average',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='flight_distance_standard_deviation',
            new_name='jasmine_flight_distance_stddev',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='flight_duration_average',
            new_name='jasmine_flight_duration_average',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='flight_duration_standard_deviation',
            new_name='jasmine_flight_duration_stddev',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='gps_data_missing_duration',
            new_name='jasmine_gps_data_missing_duration',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='radius_of_gyration',
            new_name='jasmine_gyration_radius',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='home_duration',
            new_name='jasmine_home_duration',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='stationary_fraction',
            new_name='jasmine_pause_time',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='significant_location_count',
            new_name='jasmine_significant_location_count',
        ),
        migrations.RenameField(
            model_name='summarystatisticdaily',
            old_name='significant_location_entropy',
            new_name='jasmine_significant_location_entropy',
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='jasmine_av_pause_duration',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='jasmine_obs_day',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='jasmine_obs_duration',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='jasmine_obs_night',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='jasmine_sd_pause_duration',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='summarystatisticdaily',
            name='jasmine_total_flight_time',
            field=models.FloatField(blank=True, null=True),
        ),
        
        # deleted fields
        migrations.RemoveField(
            model_name='summarystatisticdaily',
            name='acceleration_direction',
        ),
        migrations.RemoveField(
            model_name='summarystatisticdaily',
            name='accelerometer_coverage_fraction',
        ),
        migrations.RemoveField(
            model_name='summarystatisticdaily',
            name='accelerometer_signal_variability',
        ),
        migrations.RemoveField(
            model_name='summarystatisticdaily',
            name='accelerometer_univariate_summaries',
        ),
        migrations.RemoveField(
            model_name='summarystatisticdaily',
            name='awake_onset_time',
        ),
        migrations.RemoveField(
            model_name='summarystatisticdaily',
            name='call_incoming_responsiveness',
        ),
        migrations.RemoveField(
            model_name='summarystatisticdaily',
            name='device_proximity',
        ),
        migrations.RemoveField(
            model_name='summarystatisticdaily',
            name='physical_circadian_rhythm',
        ),
        migrations.RemoveField(
            model_name='summarystatisticdaily',
            name='physical_circadian_rhythm_stratified',
        ),
        migrations.RemoveField(
            model_name='summarystatisticdaily',
            name='sleep_duration',
        ),
        migrations.RemoveField(
            model_name='summarystatisticdaily',
            name='sleep_onset_time',
        ),
        migrations.RemoveField(
            model_name='summarystatisticdaily',
            name='text_incoming_responsiveness',
        ),
        migrations.RemoveField(
            model_name='summarystatisticdaily',
            name='total_power_events',
        ),
        migrations.RemoveField(
            model_name='summarystatisticdaily',
            name='total_screen_events',
        ),
        migrations.RemoveField(
            model_name='summarystatisticdaily',
            name='total_unlock_events',
        ),
    ]
