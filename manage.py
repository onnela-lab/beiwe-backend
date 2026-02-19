#!/usr/bin/env python
import multiprocessing
import os
import sys


if __name__ == "__main__":
    try:
        command = sys.argv[1]
    except IndexError:
        command = "help"
    
    if command == "test" and sys.platform == "darwin":
        # Workaround for https://code.djangoproject.com/ticket/31169
        if os.environ.get("OBJC_DISABLE_INITIALIZE_FORK_SAFETY", "") != "YES":
            print("Set OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES in your" +
                  " environment to work around use of forking in Django's" + " test runner.")
            sys.exit(1)
    multiprocessing.set_start_method("fork")
    
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.django_settings")
    from django.core.management import execute_from_command_line
    execute_from_command_line(sys.argv)
