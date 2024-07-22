import base64
import json
import logging
import uuid
from datetime import datetime
import pandas as pd

import requests
from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.db.models import Q
from rest_framework.response import Response
from simple_history.models import HistoricalRecords

from file_upload.models import UploadedFile
from model_app.abstract_base.models import BaseModelRegistry, BaseModel, BaseModelHistory
from django.contrib.gis.db import models

from model_app.base.models import ContactType, Contact
from model_app.vehicles.models import Transport, TransportBrand, TransportModel, Driver, TransportEcoClass
from notification.models import Notice
from notification import constants as c
from workflow.custom_logic import register_custom_attrs
from workflow.decorators import operation, transition
from workflow.fields.status_field import StatusField
from workflow.models import DocStatusTransition, DocStatus, DocTemplateScan, DocTemplateScanSignatory, HistoryModel, \
    BASE_HISTORY_EXCLUDED_FIELDS
from workflow.models.signature import Sign

TYPE_OF_DOC_CONST = 'Тип документа'
TS_TEXT = 'Транспорт'
ched_id_text = 'Идентификатор документа в ЦХЭД'
cant_send = 'Не удалось отправить'
status_send_to_archive = 'Отправлен в архив'
FORMAT_DATE_Z = '%Y-%m-%dT00:00:00.000Z'
celery_id_text = 'ID celery'

logger = logging.getLogger('b3.ovga_integration.models')


def get_id_set(id_set):
    id_list = id_set.split(",")
    id_set = [int(x) for x in id_list if x.isdigit()]
    if not id_set:
        return ValidationError("Неверные идентификаторы")
    return id_set


def prepare_and_send_signatures(self, transition_obj, **kwargs):
    """
    Обновляет статус, подготавливает список подписей и отправляет их через send_doc_sign_ched.
    """
    asguf_code = self.types_of_document.doc_type_enum.code
    document_class = self.types_of_document.doc_type_enum.document_class
    mimetype = self.types_of_document.doc_type_enum.mimetype
    if isinstance(self, TransportPassRequest):
        self._update_status_and_check_data(self, transition_obj)

    sign_list = [self.sign1, self.sign2]
    signature_field = self.file if hasattr(self, 'file') else self.scan

    if signature_field and signature_field.signature:
        decoded_data = base64.b64encode(signature_field.signature.tobytes()).decode('utf-8')
        sign_list.append(decoded_data)

    document_id = getattr(self, 'file_id', None) or getattr(self, 'scan_id', None)
    from integrations.ovga_integration.utils import send_doc_sign_ched
    return send_doc_sign_ched(self, transition_obj, sign_list, document_id, asguf_code, document_class, mimetype, **kwargs)


class InteragencyChecksCatalog(BaseModelRegistry):
    class Meta:
        db_table = 'interagency_checks_catalog'
        verbose_name = u'Перечень проверок по межведомственному взаимодействию'
        verbose_name_plural = u'Перечень проверок по межведомственному взаимодействию'


class ZoneValues(BaseModelRegistry):
    document_class = models.CharField('Техническое наименование типа документа', max_length=255, null=True, blank=True)
    mimetype = models.CharField('Тип данных файла документа', max_length=200, default='application/pdf')

    class Meta:
        db_table = 'zone_values'
        verbose_name = u'Значение справочника зон'
        verbose_name_plural = u'Значения справочника зон'


class ZoneReference(BaseModelRegistry):
    class Meta:
        db_table = 'zone_reference'
        verbose_name = u'Справочник зон'
        verbose_name_plural = u'Справочники зон'


class TypeDocument(BaseModel):
    type_document = models.CharField(TYPE_OF_DOC_CONST, max_length=255)
    doc_type_enum = models.ForeignKey(ZoneValues, verbose_name='Тип документа из справочника',
                                      related_name='transport_documents', on_delete=models.CASCADE)
    # Обязательность
    required = models.BooleanField(verbose_name='Обязательность', default=False)
    child_document = models.ForeignKey('self', verbose_name='Родительский тип', null=True, blank=True, on_delete=models.SET_NULL)

    class Meta:
        db_table = 'type_document'
        verbose_name = TYPE_OF_DOC_CONST
        verbose_name_plural = u'Типы документов'

    def __str__(self):
        return '{}'.format(self.type_document)


class TransportDocument(BaseModel):

    IS_MAIN_CHOISES = (
        ('yes', 'Да'),
        ('no', 'Нет'),
    )

    transport = models.ManyToManyField(Transport, verbose_name=TS_TEXT,
                                           related_name='document_transport', blank=True, db_index=True)
    types_of_document = models.ForeignKey(TypeDocument, verbose_name=TYPE_OF_DOC_CONST, null=True, blank=True,
                                          related_name='transport_documents', on_delete=models.SET_NULL)
    serial_number = models.CharField(verbose_name='Серия', max_length=25, null=True, blank=True)
    number = models.CharField(verbose_name='Номер', max_length=50, null=True, blank=True)
    issue_date = models.DateField(verbose_name='Дата выдачи', null=True, blank=True)
    date_end = models.DateField(verbose_name='Срок действия карты (дата)', null=True, blank=True)
    issue_organization = models.CharField(verbose_name='Пункт выдачи', max_length=200, null=True, blank=True)
    scan = models.ForeignKey(UploadedFile, verbose_name='Скан-копии', related_name='transport_documents',
                              null=True, blank=True, on_delete=models.SET_NULL)
    vin_number = models.CharField(verbose_name='VIN номер', max_length=25, null=True, blank=True)
    color = models.CharField(verbose_name='Цвет кузова', max_length=25, null=True, blank=True)
    kuzov_number = models.CharField(verbose_name='Номер кузова', max_length=25, null=True, blank=True)
    code_document = models.ForeignKey(InteragencyChecksCatalog, verbose_name='Код документа', null=True, blank=True,
                                      related_name='transport_documents', on_delete=models.SET_NULL)
    doc_id = models.UUIDField(editable=True, verbose_name=ched_id_text, null=True, blank=True)
    is_main = models.CharField(verbose_name='Основной\неосновной', max_length=25, choices=IS_MAIN_CHOISES, null=True, blank=True)
    status = StatusField(blank=True)
    task_id = models.UUIDField(editable=True, verbose_name=celery_id_text, null=True, blank=True)
    sign1 = models.TextField('Подпись1', null=True, blank=True)
    sign2 = models.TextField('Подпись2', null=True, blank=True)

    # Поля для ОВГА
    transport_brand_sts = models.ForeignKey(TransportBrand, verbose_name='Марка ТС для СТС', null=True, blank=True,
                                        related_name='transport_documents', on_delete=models.SET_NULL)
    transport_brand_pts = models.CharField(verbose_name='Марка ТС для ПТС', max_length=50, null=True, blank=True)
    model = models.CharField(verbose_name='Модель ТС', max_length=50, null=True, blank=True)
    type_of_transport = models.CharField(verbose_name='Тип ТС', max_length=50, null=True, blank=True)
    category = models.CharField(verbose_name='Категория', max_length=50, null=True, blank=True)
    ecoclass = models.ForeignKey(TransportEcoClass, verbose_name='Экокласс', related_name='transport_documents',
                                 on_delete=models.SET_NULL, null=True, blank=True)
    max_weight = models.FloatField('Грузоподъемность, тонн', null=True, blank=True)
    weight = models.FloatField('Масса без загрузки, кг', null=True, blank=True)

    class Meta:
        db_table = 'transport_document'
        unique_together = [('types_of_document', 'number')]
        verbose_name = u'Документ транспорт'
        verbose_name_plural = u'Документы транспорта'

    @operation(title='Загрузить файлы транспорта и водителей', operation_type='all', access_rules_from='get', icon='fas fa-archive')
    def generate_trans_docs(self, request, **kwargs):
        from integrations.ovga_integration.tasks import load_data_transport_documents
        if not request.data.get('scan'):
            return {'error_fields': None, 'error_description': "Для операции не передан файл"}, 404
        Notice.send_notice(
            request.user,
            operation_type=c.OPERATION_INFO,
            content="Запущен импорт документов из файла по существующему транспорту...",
            theme=c.THEME_SHOW
        )
        task = load_data_transport_documents.delay(file_id=request.data.get('scan'), user_id=request.user.id)
        result = {
            'task_id': task.id
        }

        return result, 200

    @transition(title='Отправить файл с подписями документа транспорта в ЦХЭД', type_operation='post')
    def send_transport_doc_signs(self, transition_obj: DocStatusTransition, **kwargs):
        return prepare_and_send_signatures(self, transition_obj, **kwargs)

    @operation(title='Изменить статус', operation_type='partial', access_rules_from='post', icon='fas fa-archive')
    def change_status_trans_docs(self, request, **kwargs):
        from integrations.ovga_integration.utils import change_status_models
        return change_status_models(request, kwargs)


class DriverDocument(BaseModel):
    IS_MAIN_CHOISES = (
        ('yes', 'Да'),
        ('no', 'Нет'),
    )

    driver = models.ForeignKey(Driver, on_delete=models.CASCADE, verbose_name='Водитель', related_name='document_driver')
    types_of_document = models.ForeignKey(TypeDocument, verbose_name=TYPE_OF_DOC_CONST, related_name='document_driver',
                                          null=True, blank=True, on_delete=models.SET_NULL)
    category = models.CharField('Категория', max_length=100, null=True, blank=True)
    series = models.CharField('Серия', max_length=255, null=True, blank=True)
    number = models.CharField('Номер', max_length=255, null=True, blank=True)
    date = models.DateField('Дата', null=True, blank=True)
    period = models.DateField(verbose_name='Срок действия (Действует до, дата)', null=True, blank=True)
    issued_by = models.CharField('Кем выдан', max_length=400, null=True, blank=True)
    issued_by_code = models.CharField(verbose_name='Код подразделения', max_length=50, blank=True, null=True)
    is_main = models.CharField(verbose_name='Основной\неосновной', max_length=25, choices=IS_MAIN_CHOISES, null=True, blank=True)
    scan = models.ForeignKey(UploadedFile, verbose_name='Скан-копии', related_name='drivers',
                             null=True, blank=True, on_delete=models.SET_NULL)
    doc_type_enum = models.ForeignKey(ZoneValues, verbose_name='Тип документа из справочника', null=True, blank=True,
                                      related_name='drivers', on_delete=models.SET_NULL)
    doc_id = models.UUIDField(editable=True, verbose_name=ched_id_text, null=True, blank=True)
    status = StatusField(blank=True)
    task_id = models.UUIDField(editable=True, verbose_name=celery_id_text, null=True, blank=True)
    sign1 = models.TextField('Подпись1', null=True, blank=True)
    sign2 = models.TextField('Подпись2', null=True, blank=True)

    class Meta:
        db_table = 'driver_document'
        verbose_name = u'Документ водителя'
        verbose_name_plural = u'Документы водителя'

    @transition(title='Отправить файл с подписями документа водителя в ЦХЭД', type_operation='post')
    def send_driver_doc_signs(self, transition_obj: DocStatusTransition, **kwargs):
        return prepare_and_send_signatures(self, transition_obj, **kwargs)

    @operation(title='Изменить статус', operation_type='partial', access_rules_from='post', icon='fas fa-archive')
    def change_status_driver_docs(self, request, **kwargs):
        from integrations.ovga_integration.utils import change_status_models
        return change_status_models(request, kwargs)


class TransportPassRequest(BaseModelHistory):
    STATUS_OF_REQUEST = (
        ('SUCCESS', 'Успешно отправлен'),
        ('ERROR', 'Отправлен с ошибками')
    )

    history = HistoricalRecords(inherit=True, bases=[HistoryModel], excluded_fields=BASE_HISTORY_EXCLUDED_FIELDS)
    mess_id = models.UUIDField(editable=True, verbose_name='Идентификатор сообщения', null=True, blank=True)
    message_date = models.DateField(blank=True, null=True, verbose_name='Дата подачи')
    status = StatusField(blank=True)
    status_request = models.CharField('Статус запроса', max_length=50, choices=STATUS_OF_REQUEST, null=True, blank=True)
    body_message = models.TextField(blank=True, null=True, verbose_name='Сообщение АИС ЭМ ТСОО')
    body_message_ovga = models.TextField(blank=True, null=True, verbose_name='Сообщение от ОВГА')
    doc_id = models.UUIDField(editable=True, verbose_name=ched_id_text, null=True, blank=True)
    task_id = models.UUIDField(editable=True, verbose_name=celery_id_text, null=True, blank=True)
    types_of_document = models.ForeignKey(TypeDocument, verbose_name=TYPE_OF_DOC_CONST, null=True, blank=True,
                                          related_name='transport_pass_request', on_delete=models.SET_NULL)
    file = models.ForeignKey(UploadedFile, verbose_name='Сформированный документ', related_name='transport_pass_request',
                             null=True, blank=True, on_delete=models.SET_NULL)
    sign1 = models.TextField('Подпись1', null=True, blank=True)
    sign2 = models.TextField('Подпись2', null=True, blank=True)

    class Meta:
        db_table = 'transport_pass_request'
        verbose_name = u'Сообщения по заявкам'
        verbose_name_plural = u'Сообщение по заявкам'

    def save(self, *args, **kwargs):
        if not self.mess_id:
            self.mess_id = uuid.uuid4()
        super().save(*args, **kwargs)

    def set_extra_values(self):
        self.pass_requests = self.transport_pass_requests.exclude(status__code='court_order_archived')

    @staticmethod
    def process_pass_requests(trans_pass_requ, status_doc_archived, status_doc_fail):
        content_list = []
        contact_type_address = ContactType.objects.filter(code="address").first() or ContactType.objects.create(code="address", name="Почтовый Адрес")
        contact_type_phone = ContactType.objects.filter(code="phone").first() or ContactType.objects.create(code="phone", name="Телефон")

        for pass_request in trans_pass_requ.exclude(status_id=status_doc_archived.id):
            trans = pass_request.transport
            request_data = {'start_date': pass_request.start_date, 'end_date': pass_request.end_date,
                            'zone': pass_request.zone_id, 'reason': pass_request.reason}

            content = Transport.validate_all_data(trans, contact_type_address, contact_type_phone, request_data)
            if content:
                content_list.append(pass_request.id)
                pass_request.body_message = content
                pass_request.status = status_doc_fail
                pass_request.request_status_send = None
                pass_request.save()

        return content_list

    @staticmethod
    def _update_status_and_check_data(self, transition_obj):
        self.status_id = transition_obj.status_id
        self.save()

        status_doc_archived, _ = DocStatus.objects.get_or_create(code='court_order_archived', defaults={'name': status_send_to_archive})
        status_doc_fail, _ = DocStatus.objects.get_or_create(code='error_status', defaults={'name': cant_send})
        trans_pass_requ = self.transport_pass_requests
        content_list = self.process_pass_requests(trans_pass_requ, status_doc_archived, status_doc_fail)

        if content_list:
            raise ValidationError(f"В запросах с указанными ID не хватает данных: {', '.join(map(str, content_list))}")

    @transition(title='Отправить файл с подписями массового запроса в ЦХЭД', type_operation='post')
    def send_to_ched_transpass_doc(self, transition_obj: DocStatusTransition, **kwargs):
        return prepare_and_send_signatures(self, transition_obj, **kwargs)

    @staticmethod
    def get_post_address_phone_number(trans):
        contact_type_address = ContactType.objects.filter(code='address').first()
        post_address = trans.participant.owner_contact.filter(contact_type=contact_type_address, is_main=True).first()
        if not post_address:
            post_address = trans.participant.owner_contact.filter(contact_type=contact_type_address).first()
        contact_type_phone = ContactType.objects.filter(code='phone').first()
        phone_number = trans.participant.owner_contact.filter(contact_type=contact_type_phone, is_main=True).first()
        if not phone_number:
            phone_number = trans.participant.owner_contact.filter(contact_type=contact_type_phone).first()
        return post_address, phone_number

    @staticmethod
    def get_applicant_pass_data_truck(trans, post_address, phone_number, pass_request):
        brand = trans.document_transport.exclude(transport_brand_pts=None).first()
        model = trans.document_transport.exclude(model=None).first()
        applicant = {
            'fullName': trans.participant.full_name,
            'inn': trans.participant.inn,
            'ogrn': trans.participant.ogrn,
            'postalAddress': post_address.value,
            'phoneNumber': phone_number.value
        }
        pass_data = {
            'startDate': pass_request.start_date.strftime(FORMAT_DATE_Z),
            'zone': pass_request.zone.code,
            'endDate': pass_request.end_date.strftime(FORMAT_DATE_Z),
            'reason': pass_request.reason
        }
        truck = {
            'grz': trans.number,
            'category': "GarbageTruck",
            'type': "GarbageTruck",
            'brand': brand.transport_brand_pts if brand else None,
            'model': model.model if model else None,
            'ecoClass': trans.document_transport.exclude(ecoclass=None).first().ecoclass.code if trans.document_transport.exclude(ecoclass=None) else None,
            'documents': []
        }
        return applicant, pass_data, truck

    @staticmethod
    def get_transport_documents(document):
        documents = {
            'documentType': document.types_of_document.doc_type_enum.code,
            'number': f'{document.serial_number} {document.number}',
            'issueDate': document.issue_date.strftime(FORMAT_DATE_Z) if document.issue_date else None,
            'validityDate': document.date_end.strftime(FORMAT_DATE_Z) if document.date_end else None,
            'documentId': document.doc_id.__str__()
        }
        return documents

    @staticmethod
    def get_driver_info(driver):
        last_name, first_name, middle_name = Transport.split_full_name(driver.name)
        driver_vu = driver.document_driver.filter(is_main='yes').exclude(deleted=True).last()
        allowed_values = ['B', 'C', 'C1', 'C2', 'C3']
        lic_cat = [val for val in driver_vu.category.split(',') if val.strip() in allowed_values]
        driver_info = {
            'surname': last_name,
            'firstName': first_name,
            'middleName': middle_name,
            'LicenseCountry': '0643',
            'license': f'{driver_vu.series}{driver_vu.number}',
            'licenseCategories': lic_cat,
            'documents': [
                {
                    'documentType': driver_vu.types_of_document.doc_type_enum.code,
                    'number': f'{driver_vu.series}{driver_vu.number}',
                    'issueDate': driver_vu.date.strftime(FORMAT_DATE_Z),
                    'validityDate': driver_vu.period.strftime(FORMAT_DATE_Z),
                    'documentId': driver_vu.doc_id.__str__()
                }
            ]
        }
        return driver_info

    @staticmethod
    def get_driver_documents(document):
        documents = {
            'documentType': document.types_of_document.doc_type_enum.code,
            'number': f'{document.series} {document.number}',
            'issueDate': document.date.strftime(FORMAT_DATE_Z),
            'validityDate': document.period.strftime(FORMAT_DATE_Z),
            'documentId': document.doc_id.__str__()
        }
        return documents

    @staticmethod
    def get_driver_info_documents(trans, other_documents, drivers):
        for driver in trans.drivers.all():
            driver_info = TransportPassRequest.get_driver_info(driver)
            for document in driver.document_driver.filter(is_main='no').exclude(deleted=True):
                documents = TransportPassRequest.get_driver_documents(document)
                other_documents.append(documents)
            drivers.append(driver_info)
        return other_documents, drivers

    @transition(title='Отправить в ОВГА', type_operation='post')
    def send_to_ovga(self, transition_obj: DocStatusTransition, **kwargs):
        self.status_id = transition_obj.status_id
        self.save()
        pass_requests = []
        contact_type_address = ContactType.objects.filter(code="address").first() or ContactType.objects.create(code="address", name="Почтовый Адрес")
        contact_type_phone = ContactType.objects.filter(code="phone").first() or ContactType.objects.create(code="phone", name="Телефон")
        status_doc_fail, _ = DocStatus.objects.get_or_create(code='error_status', defaults={'name': cant_send})
        status_doc_archived, _ = DocStatus.objects.get_or_create(code='court_order_archived', defaults={'name': status_send_to_archive})
        content_list = []
        for pass_request in self.transport_pass_requests.exclude(status_id=status_doc_archived.id):
            trans = pass_request.transport
            request_data = {'start_date': pass_request.start_date, 'end_date': pass_request.end_date,
                            'zone': pass_request.zone_id, 'reason': pass_request.reason}

            content = Transport.validate_all_data(trans, contact_type_address, contact_type_phone, request_data)

            if content:
                content_list.append(pass_request.id)
                pass_request.body_message = content
                pass_request.status = status_doc_fail
                pass_request.request_status_send = None
                pass_request.save()
                continue

            post_address, phone_number = TransportPassRequest.get_post_address_phone_number(trans)
            applicant, pass_data, truck, = TransportPassRequest.get_applicant_pass_data_truck(trans, post_address, phone_number, pass_request)

            other_documents = []
            for document in trans.document_transport.exclude(deleted=True):
                documents = TransportPassRequest.get_transport_documents(document)

                if document.is_main == 'yes':
                    truck['documents'].append(documents)
                elif document.is_main == 'no':
                    other_documents.append(documents)
            documents = {
                'documentType': self.types_of_document.doc_type_enum.code,
                'number': pass_request.p_id.__str__(),
                'issueDate': pass_request.start_date.strftime(FORMAT_DATE_Z),
                'validityDate': pass_request.end_date.strftime(FORMAT_DATE_Z),
                'documentId': self.doc_id.__str__()
            }
            other_documents.append(documents)
            drivers = []
            other_documents, drivers = TransportPassRequest.get_driver_info_documents(trans, other_documents, drivers)
            pass_requests.append({'requestId': pass_request.p_id.__str__(), 'applicant': applicant, 'pass': pass_data,
                                  'truck': truck, 'drivers': drivers, 'otherDocuments': other_documents})
        if content_list:
            raise ValidationError(f"В запросах с указанными ID не хватает данных: {', '.join(map(str, content_list))}")
        json_request = {'messageID': self.mess_id.__str__(), 'PassRequests': pass_requests,
                        'messageDate': self.message_date.strftime(FORMAT_DATE_Z)}
        from integrations.ovga_integration.utils import create_request_ovga
        return create_request_ovga(self, json_request, transition_obj, **kwargs)

    @operation(title='Изменить статус', operation_type='partial', access_rules_from='post', icon='fas fa-archive')
    def change_status_trans_pass(self, request, **kwargs):
        from integrations.ovga_integration.utils import change_status_models
        return change_status_models(request, kwargs)

    @operation(title='Сгенерировать файлы', operation_type='partial', access_rules_from='post', icon='fas fa-archive')
    def generate_files(self, request, **kwargs):
        from integrations.ovga_integration.tasks import generate_xslx_ovga
        id_set = request.GET.get('id_set')
        id_set = [int(i) for i in id_set.split(",")]
        task = generate_xslx_ovga.delay(kwargs.get('model'), id_set, request.user.id)
        return task.id, 200

    @operation(title='Сгенерировать файл', operation_type='detail', access_rules_from='post', icon='fas fa-archive')
    def generate_file(self, request, **kwargs):
        from integrations.ovga_integration.tasks import generate_xslx_ovga
        task = generate_xslx_ovga.delay(kwargs.get('model'), [kwargs.get('pk')], request.user.id)
        return task.id, 200


class PassRequests(BaseModelHistory):
    STATUS_OF_REQUEST = (
        ('SUCCESS', 'Успешно отправлен'),
        ('ERROR', 'Отправлен с ошибками')
    )

    history = HistoricalRecords(inherit=True, bases=[HistoryModel], excluded_fields=BASE_HISTORY_EXCLUDED_FIELDS)
    p_id = models.UUIDField(editable=True, null=True, blank=True, verbose_name='Идентификатор запроса')
    transport = models.ForeignKey(Transport, verbose_name=TS_TEXT, null=True, blank=True,
                             related_name='pass_requests', on_delete=models.SET_NULL)
    trans_pass_request = models.ForeignKey(TransportPassRequest, verbose_name='Заявка на пропуск', null=True, blank=True,
                                           related_name='transport_pass_requests', on_delete=models.SET_NULL)
    status = StatusField(blank=True)
    start_date = models.DateField('Дата начала действия пропуска', null=True, blank=True)
    end_date = models.DateField('Дата окончания действия пропуска', null=True, blank=True)
    zone = models.ForeignKey('ZoneReference', verbose_name='Справочник зон', null=True, blank=True,
                             related_name='pass_info', on_delete=models.SET_NULL)
    reason = models.TextField(verbose_name='Основание для выдачи пропуска', null=True, blank=True,
                              help_text='Указывается пункт из НПА на основании которого включается')
    pass_id = models.UUIDField(editable=True, verbose_name='Идентификатор пропуска', null=True, blank=True)
    pass_series_number = models.CharField(verbose_name='Серия и номер пропуска', max_length=100, blank=True, null=True)
    request_status = models.CharField(verbose_name='Статус запроса от ОВГА', max_length=100, blank=True, null=True)
    pass_status = models.CharField(verbose_name='Статус пропуска от ОВГА', max_length=100, blank=True, null=True)
    pass_date_cancellation = models.DateTimeField('Дата аннулирования', null=True, blank=True)
    cancel_reason = models.CharField(verbose_name='Причина аннулирования пропуска', max_length=100, blank=True, null=True)
    status_date = models.DateTimeField('Дата статуса', null=True, blank=True)
    request_status_send = models.CharField('Статус запроса', max_length=50, choices=STATUS_OF_REQUEST, null=True, blank=True)
    body_message = models.TextField(verbose_name='Сообщение АИС ЭМ ТСОО', null=True, blank=True)
    body_message_ovga = models.TextField(verbose_name='Сообщение от ОВГА', null=True, blank=True)

    class Meta:
        db_table = 'pass_requests'
        verbose_name = u'Заявка на пропуск'
        verbose_name_plural = u'Заявки на пропуска'

    def save(self, *args, **kwargs):
        if not self.p_id:
            self.p_id = uuid.uuid4()
        super().save(*args, **kwargs)

    @staticmethod
    def send_annul_request(json_request):
        response = Response()
        response.status_code = 500
        if settings.TEST_MODE:
            response.status_code = 200
            response.content = 'Принято'
        else:
            try:
                response = requests.post(settings.OVGA_URLS['annul_pass'], timeout=30, json=json_request)
            except requests.Timeout:
                response.content = b"Request failed: timeout expire"
        if hasattr(response, 'content'):
            try:
                content = json.dumps(json.loads(response.content), indent=4, ensure_ascii=False)
            except ValueError:
                content = response.content.decode('utf-8')
        else:
            content = ''
        return response.status_code, content

    @operation(title='Аннулировать выбранные пропуска', operation_type='partial', access_rules_from='post', icon='fas fa-archive')
    def annul_pass_requests(self, request, **kwargs):
        from workflow.classes.tables import ReflectionTableDjango
        from integrations.ovga_integration.tasks import send_annul_ovga
        # Получаем список идентификаторов из GET-параметра "id_set"
        pass_request, _ = self.get_model_and_types() if hasattr(self, 'get_model_and_types') else (self.__class__, 0)
        pass_requests = pass_request.objects.filter(id__in=get_id_set(request.GET.get("id_set", "")))
        doc_type = ReflectionTableDjango.get_for_model(ContentType.objects.get_for_model(PassRequests), PassRequests)
        pass_requests_exist = PassRequests.objects.filter(pass_id__in=pass_requests.values_list('pass_id', flat=True),
                                                          status=doc_type.default_status).values_list('id', flat=True)
        if pass_requests_exist:
            raise ValidationError(f"По пропускам с ID: {', '.join(map(str, pass_requests_exist))} уже есть заявки на аннулирование.")

        pass_requests_without_pass_id = pass_requests.filter(pass_id=None).values_list('id', flat=True)
        if pass_requests_without_pass_id:
            raise ValidationError(
                f"По пропускам с ID: {', '.join(map(str, pass_requests_without_pass_id))} не указан номер пропуска.")

        task = send_annul_ovga.delay(get_id_set(request.GET.get("id_set", "")), request.user.id, request.data['cancel_reason'])
        return task.id, 200

    @operation(title='Изменить статус', operation_type='partial', access_rules_from='post', icon='fas fa-archive')
    def change_status_pass(self, request, **kwargs):
        from integrations.ovga_integration.utils import change_status_models
        return change_status_models(request, kwargs)


class TransportExtend:
    @property
    def drivers_serials(self):
        type_document = TypeDocument.objects.filter(type_document='Водительское удостоверение').first()
        licences = ''
        for driver in self.drivers.all():
            if driver.document_driver.exists():
                vus = driver.document_driver.filter(types_of_document=type_document).exclude(deleted=True)
                if vus:
                    vu = vus.first()
                    licences += f'{vu.series} {vu.number} / '
        return licences

    @staticmethod
    def split_full_name(full_name):
        parts = full_name.split()
        last_name = parts[0] if len(parts) > 0 else ''
        first_name = parts[1] if len(parts) > 1 else ''
        middle_name = parts[2] if len(parts) > 2 else ''
        return last_name, first_name, middle_name

    @staticmethod
    def validate_transport(trans, contact_type_address, contact_type_phone):
        content = ""
        content_repeat = f'В транспорте с треком {trans.tracker_id} '
        if not trans.participant:
            content += content_repeat + 'не привязан контрагент.\r\n '
        else:
            if not trans.participant.owner_contact or not trans.participant.owner_contact.filter(
                    contact_type=contact_type_address):
                content += content_repeat + 'в контактах не указан почтовый адрес.\r\n '
            if not trans.participant.full_name:
                content += content_repeat + 'в контрагенте не указано полное наименование организации.\r\n '
            if not trans.participant.inn:
                content += content_repeat + 'в контрагенте не указан ИНН.\r\n '
            if not trans.participant.ogrn:
                content += content_repeat + 'в контрагенте не указан ОГРН.\r\n '
            if not trans.participant.owner_contact or not trans.participant.owner_contact.filter(
                    contact_type=contact_type_phone):
                content += content_repeat + 'в контактах не указан телефонный номер.\r\n '
        return content

    @staticmethod
    def validate_request_data(request_data):
        content = ""
        if not request_data["start_date"]:
            content += "Не указана Дата начала действия пропуска.\n"
        if not request_data["end_date"]:
            content += "Не указана Дата окончания действия пропуска.\n"
        if not request_data["zone"]:
            content += "Не указана зона действия транспорта.\n"
        if not request_data["reason"]:
            content += "Не указано основание включения.\n"
        return content

    @staticmethod
    def validate_trans_data(trans):
        content = ""
        content_repeat = f'В транспорте с треком {trans.tracker_id} '
        brand = trans.document_transport.exclude(transport_brand_pts=None).first()
        model = trans.document_transport.exclude(model=None).first()
        ecoclass = trans.document_transport.exclude(ecoclass=None)
        type_document = pd.DataFrame(TypeDocument.objects.filter(required=True).values('id', 'required', 'type_document', 'child_document__type_document'))
        type_document['merged_column'] = type_document.apply(lambda row: [row['type_document'], row['child_document__type_document']], axis=1)
        doc_list = trans.document_transport.values_list('types_of_document__type_document', flat=True)
        type_document['required_documents'] = type_document['merged_column'].apply(lambda x: any(item in doc_list for item in x))
        type_document.query("required_documents == False", inplace=True)
        if not trans.number:
            content += content_repeat + 'не указан государственный регистрационный знак.\r\n '
        if not trans.type or not trans.type.transport_class or not trans.type.transport_class.name:
            content += content_repeat + 'не указан класс транспорта.\r\n '
        if not brand:
            content += content_repeat + 'не указана марка транспорта в документах СТС/ПТС.\r\n '
        if not model:
            content += content_repeat + 'не указана модель транспорта в документах СТС/ПТС.\r\n '
        if not ecoclass:
            content += content_repeat + 'ни в одном приложенном документе не указан Экокласс.\r\n '
        if not type_document.empty:
            content += content_repeat + 'в документах транспорта должны быть загружены документы СТС или ПТС и Диагностическая карта.\r\n '
        if trans.drivers.all().__len__() < 1:
            content += content_repeat + 'необходимо, чтобы был привязан хотя бы 1 водитель.\r\n '
        return content

    @staticmethod
    def validate_documents(documents, sign_status_id):
        content = ""
        for document in documents:
            content_repeat = f"В документе транспорта с ID {document.id} не указаны: \r\n"
            if not document.types_of_document or not document.types_of_document.doc_type_enum:
                content += content_repeat + "тип документа. \r\n"
            if not document.doc_id:
                content += content_repeat + "идентификатор ЦХЭД. \r\n"
            if document.status_id != sign_status_id:
                content += content_repeat + "подписи. \r\n"
        return content

    @staticmethod
    def get_driver_category_error(content_repeat, document):
        type_document = TypeDocument.objects.filter(type_document='Водительское удостоверение').first()
        if document.types_of_document == type_document and document.category:
            allowed_licence = ['B', 'C', 'C1', 'C2', 'C3']
            lic_cat = ','.join([val for val in document.category.split(',') if val.strip() in allowed_licence])
            if not lic_cat:
                return content_repeat + f'для документа с ID {document.id} отсутствуют разрешенные категории.\r\n '
            return ''
        elif document.category:
            return ''
        return content_repeat + f'необходимо присвоить Категорию ВУ для документа с ID {document.id}.\r\n '

    @staticmethod
    def validate_driver_license(driver, sign_status_id):
        content_repeat = f'У водителя с ID {driver.id} '
        content = ""
        for document in driver.document_driver.filter(is_main='yes').exclude(deleted=True):
            content += Transport.get_driver_category_error(content_repeat, document)
            if not document.series:
                content += content_repeat + f'необходимо указать Серию для документа с ID {document.id}.\r\n '
            if not document.number:
                content += content_repeat + f'необходимо указать Номер ВУ для документа с ID {document.id}.\r\n '
            if not document.types_of_document or not document.types_of_document.doc_type_enum:
                content += content_repeat + f'не указан Тип документа для документа с ID: {document.id}.\r\n '
            if not document.date:
                content += content_repeat + f'необходимо указать не указана дата выдача ВУ для документа с ID: {document.id}.\r\n '
            if not document.doc_id:
                content += content_repeat + f'необходимо отправить документ с ID: {document.id} в ЦХЭД.\r\n '
            if document.status_id != sign_status_id:
                content += content_repeat + f'не подписан документ: {document.id} и подпись не прикреплена в ЦХЭД'
        return content

    @staticmethod
    def validate_other_documents(driver, sign_status_id):
        content = ""
        content_repeat = f'У водителя с ID {driver.id} '
        for document in driver.document_driver.filter(is_main='no').exclude(deleted=True):
            if not document.types_of_document or not document.types_of_document.doc_type_enum:
                content += content_repeat + f'не указан Тип документа для документа с ID: {document.id}.\r\n '
            if not document.doc_id:
                content += content_repeat + f'необходимо отправить документ в ЦХЭД с ID: {document.id}.\r\n '
            if document.status_id != sign_status_id:
                content += content_repeat + f'не подписан документ: {document.id} и подпись не прикреплена в ЦХЭД'
        return content

    @staticmethod
    def validate_drivers(drivers, sign_status_id):
        content = ""
        for driver in drivers:
            if not driver.name:
                content += f"Необходимо присвоить ФИО для водителя с ID {driver.id}. \r\n "

            if not driver.document_driver.filter(is_main='yes').exclude(deleted=True):
                content += f"Необходимо указать документ с ВУ для водителя с ID {driver.id}. \r\n "
            else:
                content += Transport.validate_driver_license(driver, sign_status_id)

            content += Transport.validate_other_documents(driver, sign_status_id)
        return content

    @staticmethod
    def validate_all_data(trans, contact_type_address, contact_type_phone, request_data):
        content = ""
        # Проверки связанные с транспортом
        content += Transport.validate_transport(trans, contact_type_address, contact_type_phone)

        # Проверки данных запроса (например, start_date, end_date, zone и reason)
        content += Transport.validate_request_data(request_data)

        content += Transport.validate_trans_data(trans)

        # Проверки связанные с документами
        documents_to_validate = trans.document_transport.exclude(Q(is_main=None) | Q(types_of_document__type_document='Диагностическая карта') | Q(deleted=True))
        sign_status = DocStatus.objects.get_or_create(code='otpravlen-v-ched', defaults={'name': 'Отправлен в ЦХЭД'})[0]
        content += Transport.validate_documents(documents_to_validate, sign_status.id)

        # Проверки связанные с водителями
        drivers_to_validate = trans.drivers.all()
        content += Transport.validate_drivers(drivers_to_validate, sign_status.id)
        return content

    @operation(title='Создать заявки на разрешения', operation_type='partial', access_rules_from='post', icon='fas fa-archive')
    def create_request(self, request, **kwargs):
        from workflow.classes.tables import ReflectionTableDjango
        # Получаем список идентификаторов из GET-параметра "id_set"

        transport, _ = self.get_model_and_types() if hasattr(self, 'get_model_and_types') else (self.__class__, 0)
        transports = transport.objects.filter(id__in=get_id_set(request.GET.get("id_set", "")))

        start_date = request.data.get('start_date')
        end_date = request.data.get('end_date')
        zone_id = request.data.get('zone')
        reason = request.data.get('reason')
        doc_type = ReflectionTableDjango.get_for_model(ContentType.objects.get_for_model(PassRequests), PassRequests)
        pass_requests = PassRequests.objects\
            .filter(end_date=end_date, start_date=start_date, status=doc_type.default_status, transport__in=transports,
                    zone=ZoneReference.objects.get(id=zone_id)).values_list('transport_id', flat=True)
        if pass_requests:
            raise ValidationError(f"На транспорт с ID: {', '.join(map(str, pass_requests))} "
                                  f"уже содержатся заявки на текущие даты по текущим зонам")

        trans_pass_request = TransportPassRequest.objects.create(message_date=datetime.now())

        pass_requests = [PassRequests(transport=trans, trans_pass_request=trans_pass_request, author=request.user,
                                      start_date=start_date, end_date=end_date, reason=reason, p_id=uuid.uuid4(),
                                      zone=ZoneReference.objects.get(id=zone_id), status=doc_type.default_status)
                         for trans in transports]

        PassRequests.objects.bulk_create(pass_requests)

        uri = f'/reg-operator/lists/mass_request_list/{trans_pass_request.id}'
        content = (
            f"Созданы заявки на разрешения. И <a href='{uri}'>Массовая заявка</a>."
        )

        Notice.send_notice(
            request.user,
            operation_type=c.OPERATION_INFO,
            content=content,
            theme=c.THEME_SHOW
        )
        return True, 200


register_custom_attrs(TransportExtend, Transport)