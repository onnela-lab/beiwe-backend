from django.core.management.base import BaseCommand
from database.user_models import Researcher


class Command(BaseCommand):
    args = ""
    help = ""

    def handle(self, *args, **options):
        new_researcher = Researcher.create_with_password("admin", "admin")
        new_researcher.elevate_to_site_admin()
