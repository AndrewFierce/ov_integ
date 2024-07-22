from functools import lru_cache

from django.apps import AppConfig


class OvgaConfig(AppConfig):
    name = 'integrations.ovga_integration'
    verbose_name = "Интеграция с ОВГА"
    additional = True
