import factory

from database.user_models import Researcher


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
