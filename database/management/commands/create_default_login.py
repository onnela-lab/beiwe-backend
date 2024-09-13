# This file is currently deprecated, uncomment it if you would like to use it locally, but it is not maintained

# from django.core.management.base import BaseCommand
# from database.user_models_researcher import Researcher


# class Command(BaseCommand):
#     args = ""
#     help = ""
    
#     def handle(self, *args, **options):
#         new_researcher = Researcher.create_with_password("admin", "admin")
#         researcher = Researcher(username="admin", password="admin")
#         researcher.set_password("abcdefg1234567!@#$%")
#         new_researcher.elevate_to_site_admin()
#         researcher.update(password_force_reset=True)  # should already be set by default
#         return researcher