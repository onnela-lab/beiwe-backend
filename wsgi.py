# This is the target file that is executed by the server to run the Beiwe Django application.
import os
import sys

from django.core.wsgi import get_wsgi_application


sys.stderr = sys.stdout

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.django_settings")
application = get_wsgi_application()  # variable must be named application
