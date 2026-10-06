import builtins, os

import django
from django.conf import settings

import config.django_settings


builtins = list(vars(builtins).keys())

# When this file is imported we attempt to load django. If django is already loaded this action
# throws a RuntimeError we catch that case and pass.
try:
    # get all the variables declared in the django settings file, exclude builtins by _identity_.
    # Django 3.2 requires that all settings be upper case.
    django_config = {
        setting_name: setting_value
        for setting_name, setting_value in vars(config.django_settings).items()
        if setting_value not in builtins and setting_name.isupper()
    }
    
    # django setup file
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.django_settings")
    settings.configure(**django_config)
    django.setup()

except RuntimeError as e:
    pass
