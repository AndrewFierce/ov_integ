from rest_framework import serializers, generics, status
from datetime import datetime

from integrations.ovga_integration.models import TransportPassRequest, PassRequests, ZoneReference
from workflow.models import LogEvent

FORMAT_DATE_Z = '%Y-%m-%dT%H:%M:%S.000Z'
FORMAT_DATE_FZ = '%Y-%m-%dT%H:%M:%S.%fZ'
FORMAT_DATE = '%Y-%m-%dT%H:%M'

ZONE_CHOICES = [
    ('SK_TTK', 'СК/ТТК'),
    ('MKAD', 'МКАД'),
]


class TransportPassRequestSerializer(serializers.ModelSerializer):
    messageId = serializers.UUIDField(source='doc_id')
    requestId = serializers.UUIDField()
    requestStatus = serializers.CharField(source='status')
    statusDate = serializers.DateTimeField(format=FORMAT_DATE_Z)
    passId = serializers.UUIDField()
    seriesNumber = serializers.CharField()
    status = serializers.CharField()
    dateCancellation = serializers.DateTimeField(format=FORMAT_DATE_Z)
    cancelReason = serializers.CharField()
    startDate = serializers.DateTimeField(format=FORMAT_DATE_Z)
    endDate = serializers.DateTimeField(format=FORMAT_DATE_Z)
    zone = serializers.ChoiceField(choices=ZONE_CHOICES)
    reason = serializers.CharField()

    class Meta:
        model = TransportPassRequest
        fields = ['messageId', 'requestId', 'requestStatus', 'statusDate', 'passId', 'seriesNumber', 'status',
                  'dateCancellation', 'cancelReason', 'cancelReason', 'startDate', 'endDate', 'zone', 'reason']


class TransportPassRequestUpdateView(generics.UpdateAPIView):
    queryset = TransportPassRequest.objects.all()
    serializer_class = TransportPassRequestSerializer

    def get_status_date(self, data):
        status_date = data.get('statusDate', '')
        if status_date:
            try:
                status_date = datetime.strptime(status_date, FORMAT_DATE_FZ)
            except ValueError:
                status_date = datetime.strptime(status_date, FORMAT_DATE)
        else:
            status_date = datetime.date().today()
        return status_date

    def get_start_end_date(self, data, pass_request):
        start_date = data.get('startDate', '')
        end_date = data.get('endDate', '')
        if start_date:
            try:
                start_date = datetime.strptime(start_date, FORMAT_DATE_FZ).date()
            except ValueError:
                start_date = datetime.strptime(start_date, FORMAT_DATE).date()
            pass_request.start_date = start_date
        if end_date:
            try:
                end_date = datetime.strptime(end_date, FORMAT_DATE_FZ).date()
            except ValueError:
                end_date = datetime.strptime(end_date, FORMAT_DATE).date()
            pass_request.end_date = end_date
        return pass_request

    def update(self, request, *args, **kwargs):
        from rest_framework.response import Response
        data = request.data
        LogEvent.objects.create(name='Получение запроса обновления из ОВГА', event_type='update', data=data)
        try:
            if data.get('requestId', ''):
                pass_requests = PassRequests.objects.filter(p_id=data.get('requestId'))
            else:
                trans_pass_requ = TransportPassRequest.objects.filter(mess_id=data.get('messageId')).first()
                pass_requests = PassRequests.objects.filter(trans_pass_request=trans_pass_requ.id)
            for pass_request in pass_requests:
                pass_request.request_status = data.get('requestStatus')

                pass_request.status_date = self.get_status_date(data)

                pass_request.pass_id = data.get('passId')
                pass_request.pass_series_number = data.get('seriesNumber')
                pass_request.pass_status = data.get('status')
                date_cancellation = data.get('dateCancellation', '')
                if date_cancellation:
                    try:
                        date_cancellation = datetime.strptime(date_cancellation, FORMAT_DATE_FZ)
                    except ValueError:
                        date_cancellation = datetime.strptime(date_cancellation, FORMAT_DATE)
                    pass_request.pass_date_cancellation = date_cancellation
                pass_request.cancel_reason = data.get('cancelReason')

                pass_request = self.get_start_end_date(data, pass_request)
                zone_code = data.get('zone')
                zone = ZoneReference.objects.filter(code=zone_code).first()
                pass_request.zone = zone
                pass_request.reason = data.get('reason')
                pass_request.save()
        except PassRequests.DoesNotExist:
            return Response({"error": "PassRequest with provided ID does not exist"}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        return Response('Информация обновлена.')