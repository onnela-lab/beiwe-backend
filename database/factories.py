import factory
from .tableau_api_models import SummaryStatisticDaily
from .user_models import Participant
from .study_models import Study

class ParticipantFactory(factory.Factory):
    class Meta:
        model = Participant
    # first_name = factory.Faker('first_name')
    # last_name = factory.Faker('last_name')


class StudyFactory(factory.Factory):
    class Meta:
        model = Study


class SummaryFactory(factory.Factory):
    class Meta:
        model = SummaryStatisticDaily
