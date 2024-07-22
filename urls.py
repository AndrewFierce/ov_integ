from django.urls import path

from integrations.ovga_integration.serializers import TransportPassRequestUpdateView

urlpatterns = [
    path('transport-pass-request/', TransportPassRequestUpdateView.as_view(), name='trans_pass_update'),
]