import os

import django
from django.conf import settings

from config.django_settings import DATABASES, INSTALLED_APPS, SECRET_KEY, TIME_ZONE

try:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.django_settings")

    settings.configure(
        SECRET_KEY=SECRET_KEY,
        DATABASES=DATABASES,
        TIME_ZONE=TIME_ZONE,
        INSTALLED_APPS=INSTALLED_APPS
    )

    django.setup()
except Exception as e:
    print("Not-critical exception:", e)
