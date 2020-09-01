import factory

from database.tableau_api_models import SummaryStatisticDaily
from database.user_models import Researcher

import factory
from database.tableau_api_models import SummaryStatisticDaily
from database.user_models import Participant
from database.study_models import Study
from random import randint
from random import random
from faker import Faker
from datetime import date, datetime
fake = Faker()


class ResearcherFactory(factory.DjangoModelFactory):
    class Meta:
        model = Researcher

    password = '1'  # See `.set_password`
    salt = '1'  # See `.set_password`
    username = factory.Faker('user_name')

    @factory.post_generation
    def set_password(self, create, extracted, **kwargs):
        """
        Beiwe's `CommonModel` overrides the `.save` method in a way that breaks the usual Django-
        factory_boy integration. The `password` and `salt` values declared in the attributes
        prevents the `.save` method from breaking this model generation, and this method set an
        actually valid password hash and salt for normal use.
        """
        self.set_password('1')
        self.save()


class SummaryStatisticDailyFactory(factory.DjangoModelFactory):
    class Meta:
        model = SummaryStatisticDaily

    date = factory.LazyAttribute((lambda _: fake.date_of_birth())) #  not actually a DOB, but a generates a reasonable range
    distance_diameter = factory.LazyAttribute(lambda _: randint(0,1000))
    distance_from_home = factory.LazyAttribute(lambda _: randint(0,1000))
    distance_traveled = factory.LazyAttribute(lambda _: randint(0,1000))
    flight_distance_average = factory.LazyAttribute(lambda _: randint(0,1000))
    flight_distance_standard_deviation = factory.LazyAttribute(lambda _: randint(0,10000))
    flight_duration_average = factory.LazyAttribute(lambda _: randint(0,10000))
    flight_duration_standard_deviation = factory.LazyAttribute(lambda _: randint(0,10000))
    gps_data_missing_duration = factory.LazyAttribute(lambda _: randint(0,10000))
    home_duration = factory.LazyAttribute(lambda _: randint(0,10000))
    physical_circadian_rhythm = factory.LazyAttribute(lambda _: random())
    physical_circadian_rhythm_stratified = factory.LazyAttribute(lambda _: random())
    radius_of_gyration = factory.LazyAttribute(lambda _: randint(0,10000))
    significant_location_count = factory.LazyAttribute(lambda _: randint(0,10000))
    significant_location_entropy = factory.LazyAttribute(lambda _: randint(0,10000))
    stationary_fraction = factory.LazyAttribute((lambda _: factory.Faker('text')))
    text_incoming_count = factory.LazyAttribute(lambda _: randint(0,10000))
    text_incoming_degree = factory.LazyAttribute(lambda _: randint(0,10000))
    text_incoming_length = factory.LazyAttribute(lambda _: randint(0,10000))
    text_incoming_responsiveness = factory.LazyAttribute(lambda _: randint(0,10000))
    text_outgoing_count = factory.LazyAttribute(lambda _: randint(0,10000))
    text_outgoing_degree = factory.LazyAttribute(lambda _: randint(0,10000))
    text_outgoing_length = factory.LazyAttribute(lambda _: randint(0,10000))
    text_reciprocity = factory.LazyAttribute(lambda _: randint(0,10000))
    call_incoming_count = factory.LazyAttribute(lambda _: randint(0,10000))
    call_incoming_degree = factory.LazyAttribute(lambda _: randint(0,10000))
    call_incoming_duration = factory.LazyAttribute(lambda _: randint(0,10000))
    call_incoming_responsiveness = factory.LazyAttribute(lambda _: randint(0,10000))
    call_outgoing_count = factory.LazyAttribute(lambda _: randint(0,10000))
    call_outgoing_degree = factory.LazyAttribute(lambda _: randint(0,10000))
    call_outgoing_duration = factory.LazyAttribute(lambda _: randint(0,10000))
    acceleration_direction = factory.LazyAttribute((lambda _:fake.sentence()))
    accelerometer_coverage_fraction = factory.LazyAttribute((lambda _:fake.sentence()))
    accelerometer_signal_variability = factory.LazyAttribute((lambda _:fake.sentence()))
    accelerometer_univariate_summaries = factory.LazyAttribute(lambda _: random() * 100)
    device_proximity = True
    total_power_events = factory.LazyAttribute(lambda _: randint(0,10000))
    total_screen_events = factory.LazyAttribute(lambda _: randint(0,10000))
    total_unlock_events = factory.LazyAttribute(lambda _: randint(0,10000))
    awake_onset_time = factory.LazyAttribute((lambda _:fake.date_time()))
    sleep_duration = factory.LazyAttribute(lambda _: randint(0,10000))
    sleep_onset_time = factory.LazyAttribute((lambda _:fake.date_time()))

