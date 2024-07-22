from django.contrib import admin
from big3_data_main_app.custom_admin import custom_admin_site
from integrations.ovga_integration.models import InteragencyChecksCatalog, ZoneValues, ZoneReference
from model_app.admin import BaseModelRegistryAdmin


@admin.register(InteragencyChecksCatalog, site=custom_admin_site)
class InteragencyChecksCatalogAdmin(BaseModelRegistryAdmin):
    ...


@admin.register(ZoneValues, site=custom_admin_site)
class ZoneValuesAdmin(BaseModelRegistryAdmin):
    ...


@admin.register(ZoneReference, site=custom_admin_site)
class ZoneReferenceAdmin(BaseModelRegistryAdmin):
    ...
