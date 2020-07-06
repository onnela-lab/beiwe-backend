from django.db import models
from database.common_models import AbstractModel


class SchemaTableauAPI(AbstractModel):
    participant_id = models.CharField(max_length=8)
    study_id = models.CharField(max_length=24)
    date = models.DateField()
    distance_diameter = models.IntegerField()
    distance_from_home = models.IntegerField()
    distance_travelled = models.IntegerField()
    flight_distance_average = models.IntegerField()
    flight_distance_standard_deviation = models.IntegerField()
    flight_duration_average = models.IntegerField()
    flight_duration_standard_deviation = models.IntegerField()
    gps_data_missing_duration = models.IntegerField()
    home_duration = models.IntegerField()
    physical_circadian_rhythm = models.FloatField() #fixed point?
    physical_circadian_rhythm_stratified = models.FloatField() #fixed point?
    radius_of_gyration = models.IntegerField()
    significant_location_count = models.IntegerField()
    significant_location_entroy = models.IntegerField()
    stationary_fraction = models.IntegerField()
    text_incoming_count = models.IntegerField()
    text_incoming_degree = models.IntegerField()
    text_incoming_length = models.IntegerField()
    text_incoming_responsiveness = models.IntegerField()
    text_outgoing_count = models.IntegerField()
    text_outgoing_degree = models.IntegerField()
    text_outgoing_length = models.IntegerField()
    text_reciprocity = models.IntegerField()
    call_incoming_count = models.IntegerField()
    call_incoming_degree = models.IntegerField()
    call_incoming_duration = models.IntegerField()
    call_incoming_responsiveness = models.IntegerField()
    call_outgoing_count = models.IntegerField()
    call_outgoing_degree = models.IntegerField()
    call_outgoing_duration = models.IntegerField()
    acceleration_direction = models.CharField(max_length=30) #len?
    accelerometer_coverage_fraction = models.CharField(max_length=10) #len?
    accelerometer_signal_variability = models.CharField(max_length=10) #len?
    accelerometer_univariate_summaries = models.FloatField()
    device_proximity = models.BooleanField()
    total_power_events = models.IntegerField()
    total_screen_events = models.IntegerField()
    total_unlock_events = models.IntegerField()
    awake_onset_time = models.CharField(max_length=12) #len?
    sleep_duration = models.IntegerField()
    sleep_onset_time = models.CharField(max_length=12) #len?

