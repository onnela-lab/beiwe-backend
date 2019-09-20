# -*- coding: utf-8 -*-

from django.apps import AppConfig


class DatabaseConfig(AppConfig):
    name = 'database'

    def ready(self):
        from database import signals
