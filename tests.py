import uuid
from datetime import datetime, date
from io import BytesIO

import pandas as pd
from django.conf import settings
from django.test import override_settings

from access_management_django.tests.factories import RoleFactory

from django.http import HttpRequest
from django.test import TestCase
from django.urls import reverse
from rest_framework import status
from file_upload.models import UploadedFile

from integrations.factories import DriverDocumentFactory, TransportDocumentFactory, TransportPassRequestFactory, \
    ZoneValuesFactory, TypeDocumentFactory, ZoneReferenceFactory, PassRequestsFactory
from integrations.ovga_integration.models import TransportDocument, TransportPassRequest, PassRequests, DriverDocument, \
    ZoneValues, TypeDocument
from integrations.ovga_integration.tasks import load_data_transport_documents, send_file_sign_ched, send_request_ovga, \
    send_annul_ovga, change_stat_model_task
from model_app.vehicles.models import TransportBase, Transport, TransportBrand, TransportModel
from statistic.tests.test_confirmations import create_doc_type_and_transition

from tests.factories import DocStatusFactory, DocStatusTransitionFactory, DocTypeFactory, ContactTypeFactory, \
    ContactFactory, ParticipantFactory, TransportTypeFactory, DriverFactory, TransportFactory, UserFactory, \
    DocStatusTransitionRulesFactory, TransportEcoClassFactory, UploadedFileFactory
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from rest_framework.test import APIClient


for_ovga = 'Для ОВГА'


class LoadDataTransportTest(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.user = UserFactory(last_name='Иванов', first_name='Иван')
        trans_base, _ = TransportBase.objects.get_or_create(address='Тестовый адрес для загрузки')
        cls.trans1, _ = Transport.objects.get_or_create(tracker_id='201422.123', defaults={'number': 'А023АН799'})
        cls.trans2, _ = Transport.objects.get_or_create(tracker_id='201422.456', defaults={'number': 'А023АН799'})
        cls.trans3, _ = Transport.objects.get_or_create(tracker_id='10022261.123', defaults={'number': 'А133УН799', 'transport_base_id': trans_base.id})
        trans_brand1, _ = TransportBrand.objects.get_or_create(name='МАЗ')
        trans_brand2, _ = TransportBrand.objects.get_or_create(name='VOLVO')
        TransportModel.objects.get_or_create(name='5550В2', brand_id=trans_brand1.id)
        TransportModel.objects.get_or_create(name='FМ-TRUCK', brand_id=trans_brand2.id)
        with open('integrations/ovga_integration/load_transport_test.xlsx', 'rb') as transport_file:
            data_transport = transport_file.read()
        cls.up_file = UploadedFile.create('Data_Transport', data_transport)

    def test_load_data(self):
        load_data_transport_documents(self.up_file.id, self.user.id)
        trans_docs = TransportDocument.objects.filter(vin_number='X89787810E0AP0006', number='215643').first()
        self.assertTrue(trans_docs)
        self.assertEqual(trans_docs.transport.first(), self.trans3)
        self.assertEqual(trans_docs.transport_brand_pts, 'ВОЛЬВО')
        self.assertEqual(trans_docs.model, 'ЭТП-01')
        self.assertEqual(trans_docs.type_of_transport, 'Специальное')
        self.assertEqual(trans_docs.category, 'C/N3')
        self.assertEqual(trans_docs.ecoclass.name, 'Евро 4')
        self.assertEqual(trans_docs.issue_organization, 'ООО ХАРТИЯ')
        self.assertEqual(trans_docs.serial_number, '9932')
        self.assertEqual(trans_docs.number, '215643')
        self.assertEqual(trans_docs.issue_date, datetime.strptime('22.04.2021', '%d.%m.%Y').date())
        self.assertEqual(trans_docs.is_main, 'yes')
        trans_docs = TransportDocument.objects.filter(vin_number='Х89341201H0АА3111', number='215947').first()
        self.assertTrue(trans_docs)
        self.assertEqual(trans_docs.transport.first(), self.trans1)
        self.assertEqual(trans_docs.transport_brand_pts, 'МАЗ')
        self.assertEqual(trans_docs.model, 'МК 3412-01')
        self.assertEqual(trans_docs.type_of_transport, 'Специализированный прочее')
        self.assertEqual(trans_docs.category, 'C/N3')
        self.assertEqual(trans_docs.ecoclass.name, 'Евро 4')
        self.assertEqual(trans_docs.issue_organization, 'ООО ХАРТИЯ')
        self.assertEqual(trans_docs.serial_number, '9932')
        self.assertEqual(trans_docs.number, '215947')
        self.assertEqual(trans_docs.issue_date, datetime.strptime('27.04.2021', '%d.%m.%Y').date())
        self.assertEqual(trans_docs.is_main, 'yes')

        trans_docs = TransportDocument.objects.filter(serial_number='16НО', number='230228').first()
        self.assertTrue(trans_docs)
        self.assertEqual(trans_docs.transport.first(), self.trans3)
        self.assertEqual(trans_docs.issue_date, datetime.strptime('22.04.2021', '%d.%m.%Y').date())
        self.assertEqual(trans_docs.is_main, 'yes')
        self.assertEqual(trans_docs.transport_brand_pts, 'ЭТП-01')
        trans_docs = TransportDocument.objects.filter(serial_number='62ОС', number='765042').first()
        self.assertTrue(trans_docs)
        self.assertEqual(trans_docs.transport.first(), self.trans1)
        self.assertEqual(trans_docs.issue_date, datetime.strptime('28.08.2017', '%d.%m.%Y').date())
        self.assertEqual(trans_docs.is_main, 'yes')
        self.assertEqual(trans_docs.transport_brand_pts, 'МК-3412-01 на шасси МАЗ-5550В2')

        doc_type_enum_id = ZoneValues.objects.get_or_create(code=10367, name='Паспорт транспортного средства')[0].id
        type_id = TypeDocument.objects.get_or_create(type_document='Диагностическая карта', doc_type_enum_id=doc_type_enum_id)[0].id
        trans_docs = TransportDocument.objects.filter(types_of_document_id=type_id, number='087531042201428').first()
        self.assertTrue(trans_docs)
        self.assertEqual(trans_docs.transport.first(), self.trans3)
        self.assertEqual(trans_docs.date_end, datetime.strptime('28.09.2023', '%d.%m.%Y').date())
        self.assertEqual(trans_docs.is_main, 'yes')
        trans_docs = TransportDocument.objects.filter(types_of_document_id=type_id, number='081571042200653').first()
        self.assertTrue(trans_docs)
        self.assertEqual(trans_docs.transport.first(), self.trans1)
        self.assertEqual(trans_docs.date_end, datetime.strptime('18.05.2023', '%d.%m.%Y').date())
        self.assertEqual(trans_docs.is_main, 'yes')

        doc_type_enum_id = ZoneValues.objects.get_or_create(code=10901, name='Документ(ы), на основании которого(-ых) действует представитель заявителя')[0].id
        type_id = TypeDocument.objects.get_or_create(type_document='Лизингодатель', doc_type_enum_id=doc_type_enum_id)[0].id
        trans_docs = TransportDocument.objects.filter(types_of_document_id=type_id, number='201422.123', issue_organization='АО "Регион Лизинг"').first()
        self.assertFalse(trans_docs.is_main)
        self.assertEqual(trans_docs.transport.first(), self.trans1)
        trans_docs = TransportDocument.objects.filter(types_of_document_id=type_id, number='10022261.123', issue_organization='АО "Регион Лизинг"').first()
        self.assertFalse(trans_docs.is_main)
        self.assertEqual(trans_docs.transport.first(), self.trans3)

        driver_doc = DriverDocument.objects.filter(series='3415', number='734176').first()
        self.assertTrue(driver_doc)
        self.assertEqual(driver_doc.category, 'B,C,CE')
        self.assertEqual(driver_doc.date, datetime.strptime('04.12.2013', '%d.%m.%Y').date())
        self.assertEqual(driver_doc.period, datetime.strptime('04.12.2023', '%d.%m.%Y').date())
        self.assertEqual(driver_doc.issued_by_code, '3411')
        self.assertEqual(driver_doc.is_main, 'yes')
        self.assertEqual(driver_doc.driver.name, 'Устинов Василий Александрович')
        self.assertEqual(driver_doc.driver.birthday, datetime.strptime('14.12.1972', '%d.%m.%Y').date())
        driver_doc = DriverDocument.objects.filter(series='9902', number='137572').first()
        self.assertTrue(driver_doc)
        self.assertEqual(driver_doc.category, 'B,B1,C,C1,M')
        self.assertEqual(driver_doc.date, datetime.strptime('30.08.2018', '%d.%m.%Y').date())
        self.assertEqual(driver_doc.period, datetime.strptime('30.08.2028', '%d.%m.%Y').date())
        self.assertEqual(driver_doc.issued_by_code, '6830')
        self.assertEqual(driver_doc.is_main, 'yes')
        self.assertEqual(driver_doc.driver.name, 'Конкин Андрей Александрович')
        self.assertEqual(driver_doc.driver.birthday, datetime.strptime('06.02.1978', '%d.%m.%Y').date())


class DriverDocumentTestCase(TestCase):

    def __init__(self, methodName: str = ...):
        super().__init__(methodName)
        self.transition = None
        self.status_submitted = None
        self.status_draft = None

    @staticmethod
    def generate_random_signature(length):
        import random
        import string
        # Определите набор символов, которые могут быть включены в подпись
        characters = string.ascii_letters + string.digits + "!@#$%^&*()_+=-"

        # Генерируйте случайную строку определенной длины
        random_signature = ''.join(random.choice(characters) for _ in range(length))

        return random_signature

    def setUp(self):
        # Создаем необходимые фабрики
        self.user = UserFactory(last_name='Иванов', first_name='Иван')
        self.uuid4_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
        create_doc_type_and_transition(self, 'driverdocument')

    @override_settings(CELERY_TASK_ALWAYS_EAGER=True)
    def test_send_to_ched_docs_signs(self):
        # Создаем объект DriverDocument с фабрикой
        driver_document = DriverDocumentFactory(status=self.status_draft)

        # Вызываем метод send_to_ched_transport_doc
        result, status_code = driver_document.send_driver_doc_signs(driver_document, self.transition, user_id=self.user.id)

        # Проверяем, что результат вернул True и статус изменился на ожидаемый
        self.assertTrue(result)
        driver_document.refresh_from_db()
        self.assertRegex(result, self.uuid4_pattern)
        self.assertEqual(status_code, 200)
        transport_document = TransportDocumentFactory(status=self.status_draft)

        # Вызываем метод send_to_ched_transport_doc
        result, status_code = transport_document.send_transport_doc_signs(transport_document, self.transition)

        # Проверяем, что результат вернул True и статус изменился на ожидаемый
        self.assertTrue(result)
        self.assertRegex(result, self.uuid4_pattern)
        self.assertEqual(status_code, 200)

        # Create a TransportPassRequest instance using the factory
        trans_pass_request = TransportPassRequestFactory(status=self.status_draft)
        result, status_code = trans_pass_request.send_to_ched_transpass_doc(trans_pass_request, self.transition, **{'user_id': self.user.id})

        # Assert that the result is as expected
        self.assertTrue(result)
        self.assertRegex(result, self.uuid4_pattern)
        self.assertEqual(status_code, 200)
        trans_pass_request.refresh_from_db()
        self.assertEqual(trans_pass_request.status.id, self.status_draft.id)
        up_file = UploadedFileFactory()
        trans_pass_request.file_id = up_file.id
        trans_pass_request.sign1 = DriverDocumentTestCase.generate_random_signature(100)
        trans_pass_request.save()
        trans_pass_request.send_to_ched_transpass_doc(trans_pass_request, self.transition, **{'user_id': self.user.id})
        trans_pass_request.refresh_from_db()
        self.assertNotEquals(trans_pass_request.status.id, self.status_draft.id)

    def test_check_task_file_sign(self):
        # Создаем объект DriverDocument с фабрикой
        driver_document = DriverDocumentFactory(status=self.status_draft, scan=None)
        status = send_file_sign_ched(driver_document.__class__.__name__, driver_document.id, driver_document.scan_id,
                                     self.status_draft.id, self.status_submitted.id, settings.CHED_CREDENTIALS['document_class'],
                                     '14119', 'application/pdf', [driver_document.sign1, driver_document.sign2], self.user.id)
        driver_document.refresh_from_db()
        self.assertFalse(status)
        self.assertEqual(driver_document.status, self.status_draft)

        driver_document = DriverDocumentFactory(status=self.status_draft)
        status = send_file_sign_ched(driver_document.__class__.__name__, driver_document.id, driver_document.scan_id,
                                     self.status_draft.id, self.status_submitted.id, settings.CHED_CREDENTIALS['document_class'],
                                     '14119', 'application/pdf', [driver_document.sign1, driver_document.sign2], self.user.id)
        driver_document.refresh_from_db()
        self.assertRegex(driver_document.doc_id.__str__(), self.uuid4_pattern)
        self.assertFalse(status)
        self.assertEqual(driver_document.status, self.status_draft)
        driver_document.sign1 = DriverDocumentTestCase.generate_random_signature(100)
        driver_document.sign2 = DriverDocumentTestCase.generate_random_signature(200)
        driver_document.save()
        status = send_file_sign_ched(driver_document.__class__.__name__, driver_document.id, driver_document.scan_id,
                                     self.status_draft.id, self.status_submitted.id, settings.CHED_CREDENTIALS['document_class'],
                                     '14119', 'application/pdf', [driver_document.sign1, driver_document.sign2], self.user.id)
        driver_document.refresh_from_db()
        self.assertTrue(status)
        self.assertEqual(driver_document.status, self.status_submitted)


class TransportPassRequestTest(TestCase):

    def set_contact(self, participant_id):
        ContactFactory(**{'contact_type': self.address_type, 'value': '127000 г. Москва, Вяземская, 5',
                          'comment': for_ovga, 'is_main': True, 'participant_id': participant_id})
        ContactFactory(**{'contact_type': self.phone_type, 'value': '89639877654', 'comment': for_ovga, 'is_main': True,
                          'participant_id': participant_id})

    def setUp(self):
        self.user = UserFactory()
        self.participant_data = {
            'name': 'ООО "ГК СЭТ"',
            'inn': '9729022231',
            'kpp': '772901001',
            'ogrn': '1167746794050',
            'is_active': True,
            'is_resident': True,
            'okpo': '04162685',
            'registration_date': '2021-10-29',
            'ownership_mode': 'ooo',
            'short_name': 'ООО "ГРУППА КОМПАНИЙ СОВРЕМЕННЫЕ ЭКОЛОГИЧЕСКИЕ ТЕХНОЛОГИИ"',
            'full_name': 'ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ "ГРУППА КОМПАНИЙ СОВРЕМЕННЫЕ ЭКОЛОГИЧЕСКИЕ ТЕХНОЛОГИИ"',
        }

        participant = ParticipantFactory(**self.participant_data)

        driver_data = {
            'license_number': '77777777',
            'name': 'Грефов Франц Сберович',
        }
        self.driver = DriverFactory(**driver_data)

        vu = ZoneValuesFactory(**{'name': 'Водительское удостоверение', 'code': '10269'})
        passport = ZoneValuesFactory(**{'name': 'Паспорт транспортного средства', 'code': '10367'})
        unknow = ZoneValuesFactory(**{'name': 'Неизвестный договор', 'code': '10687'})
        reg_ts = ZoneValuesFactory(**{'name': 'Свидетельство о регистрации ТС', 'code': '347'})
        self.type_doc1 = TypeDocumentFactory(**{'type_document': 'Водительское удостоверение', 'doc_type_enum': vu})
        self.type_doc2 = TypeDocumentFactory(**{'type_document': 'Диагностическая карта', 'doc_type_enum': passport, 'required': True})
        TypeDocumentFactory(**{'type_document': 'Другие документы', 'doc_type_enum': unknow})
        TypeDocumentFactory(**{'type_document': 'Подпись', 'doc_type_enum': unknow})
        self.type_doc5 = TypeDocumentFactory(**{'type_document': 'ПТС', 'doc_type_enum': reg_ts})
        self.type_doc6 = TypeDocumentFactory(**{'type_document': 'СТС', 'doc_type_enum': reg_ts, 'required': True, 'child_document_id': self.type_doc5.id})
        DriverDocumentFactory(**{'driver': self.driver, 'types_of_document': self.type_doc1, 'is_main': 'yes', 'series': '7635', 'number': '821719'})

        transport_data = {
            'year': 2020,
            'name': 'А477НР57 2020',
            'tracker_id': '10075950.10012',
            'number': 'А477НР57',
            'server': None,
            'average_waste_density': None,
            'base': 'ООО "ГРИН ЛЭНД"',
            'autopark': None,
            'garage_number': None,
            'telemetry_blocked': False,
            'assignment': Transport.ASSIGNMENT_TAKEOUT,
            'author': self.user,
            'participant': participant,  # Замените на соответствующий объект участника
            'datetime_create': datetime(2023, 8, 3, 18, 20, 11, 701158),
            'datetime_update': datetime(2023, 8, 17, 9, 23, 53, 175280),
            'datetime_delete': None,
            'deleted': False,
            'deleted_by': None,
            'transport_base': None,
            'latest_telemetry': None,
            'latest_telemetry_updated': None,
            'bnso_devices': None,
        }

        self.transport = TransportFactory(**transport_data)
        self.transport.drivers.set([self.driver])

        TransportDocumentFactory(**{'transport': [self.transport], 'types_of_document': self.type_doc2, 'is_main': 'yes'})
        TransportDocumentFactory(**{'transport': [self.transport], 'types_of_document': self.type_doc6, 'is_main': 'yes'})
        TransportDocumentFactory(**{'transport': [self.transport], 'types_of_document': self.type_doc5, 'is_main': 'yes'})

        zone_data = {'name': 'СК/ТТК', 'code': 'SK_TTK'}
        self.zone = ZoneReferenceFactory(**zone_data)

        self.address_type = ContactTypeFactory(**{'name': 'Почтовый Адрес', 'code': 'address'})
        self.phone_type = ContactTypeFactory(**{'name': 'Телефон', 'code': 'phone'})
        self.set_contact(participant.id)
        self.pass_request = PassRequestsFactory()
        self.transition_status_to_1 = DocStatusFactory(code='doc_status_to_1')
        self.transition_status_1 = DocStatusFactory(code='doc_status_1')
        model_name = 'transportpassrequest'
        table = ContentType.objects.filter(model=model_name).first()
        table1 = ContentType.objects.filter(model='passrequests').first()
        self.status = DocStatusFactory()
        self.doc_type = DocTypeFactory(table=table, default_status=self.status, code=f'doc_type_{model_name}')
        DocTypeFactory(table=table1, default_status=self.status, code='doc_type_passrequests')
        self.transition1 = DocStatusTransitionFactory(doc_type=self.doc_type,
                                                      code='transition_code_1',
                                                      status=self.transition_status_1,
                                                      status_to=self.transition_status_to_1,
                                                      pre_transition='test_pre_transition',
                                                      post_transition='test_post_transition')
        self.uuid4_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'

    def test_create_request(self):
        http_request = HttpRequest()
        http_request.GET = {'id_set': f'{self.transport.id}'}
        http_request.data = {'end_date': '2023-08-24', 'reason': 'НПА', 'start_date': '2023-08-02', 'zone': self.zone.id}
        http_request.user = self.user
        responce = self.transport.create_request(http_request)
        self.assertTrue(responce[0])
        self.assertEqual(responce[1], 200)
        start_date = datetime.strptime('2023-08-02', '%Y-%m-%d').date()
        end_date = datetime.strptime('2023-08-24', '%Y-%m-%d').date()
        pass_request = PassRequests.objects.filter(transport_id=self.transport.id, start_date=start_date, end_date=end_date, reason='НПА', zone_id=self.zone.id)
        self.assertTrue(pass_request)

    @override_settings(CELERY_TASK_ALWAYS_EAGER=True)
    def test_generate_files(self):
        trans_pass_request = TransportPassRequestFactory()
        PassRequestsFactory(transport=self.transport, trans_pass_request=trans_pass_request)
        http_request = HttpRequest()
        http_request.GET = {'id_set': f'{self.transport.id}'}
        http_request.user = self.user
        trans_pass_request.generate_file(http_request, **{'model': 'transportpassrequest', 'pk': trans_pass_request.id})
        trans_pass_request.refresh_from_db()
        file_content = trans_pass_request.file.file.read()
        excel_data = BytesIO(file_content)
        df = pd.read_excel(excel_data, engine='openpyxl', dtype=str)
        self.assertEqual(df['Государственный регистрационный знак'][0], 'А477НР57')
        self.assertEqual(df['Полное наименование юридического лица/индивидуального предпринимателя'][0], 'ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ "ГРУППА КОМПАНИЙ СОВРЕМЕННЫЕ ЭКОЛОГИЧЕСКИЕ ТЕХНОЛОГИИ"')
        self.assertEqual(df['Серия, номер водительского удостоверения водителя транспортного средства'][0], '7635 821719')
        self.assertEqual(df['Телефон юридического лица/индивидуального предпринимателя'][0], '89639877654')
        self.assertEqual(df['Почтовый адрес юридического лица/индивидуального предпринимателя'][0], '127000 г. Москва, Вяземская, 5')
        self.assertEqual(df['ОГРН/ОГРНИП/ИНН юридического лица/индивидуального предпринимателя/физического лица'][0], '1167746794050 / 9729022231')


    @staticmethod
    def _create_pass_request(transport, zone, user):
        http_request = HttpRequest()
        http_request.GET = {'id_set': f'{transport.id}'}
        http_request.data = {'end_date': '2023-08-24', 'reason': 'НПА', 'start_date': '2023-08-02', 'zone': zone.id}
        http_request.user = user
        transport.create_request(http_request)

    def test_send_to_ovga(self):
        transport = TransportFactory(tracker_id='10076950.10012', type=None)
        self._create_pass_request(transport, self.zone, self.user)
        trans_pass_request = TransportPassRequest.objects.filter(transport_pass_requests__in=transport.pass_requests.all()).first()
        content_repeat = f'В транспорте с треком {transport.tracker_id} '
        try:
            trans_pass_request.send_to_ovga(trans_pass_request, self.transition1)
        except ValidationError:
            pass_request = trans_pass_request.transport_pass_requests.first()
            expected_phrase = content_repeat + 'не привязан контрагент.'
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = 'Не указана Дата начала действия пропуска.'
            self.assertFalse(expected_phrase in pass_request.body_message)
            expected_phrase = 'Не указана Дата окончания действия пропуска.'
            self.assertFalse(expected_phrase in pass_request.body_message)
            expected_phrase = 'Не указана зона действия транспорта.'
            self.assertFalse(expected_phrase in pass_request.body_message)
            expected_phrase = 'Не указано основание включения.'
            self.assertFalse(expected_phrase in pass_request.body_message)
            expected_phrase = content_repeat + 'не указан государственный регистрационный знак.'
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = content_repeat + 'не указан класс транспорта.'
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = content_repeat + 'не указана марка транспорта в документах СТС/ПТС.'
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = content_repeat + 'не указана модель транспорта в документах СТС/ПТС.'
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = content_repeat + 'ни в одном приложенном документе не указан Экокласс.'
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = content_repeat + 'в документах транспорта должны быть загружены документы СТС или ПТС и Диагностическая карта.'
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = content_repeat + 'необходимо, чтобы был привязан хотя бы 1 водитель.'
            self.assertTrue(expected_phrase in pass_request.body_message)
        participant = ParticipantFactory(ogrn=None, type=None, regions=None)
        transport_type = TransportTypeFactory()
        trans_doc1 = TransportDocumentFactory(transport=[transport], types_of_document=None, serial_number=None, number=None,
                                 issue_date=None, date_end=None, issue_organization=None, vin_number=None, color=None,
                                 kuzov_number=None, doc_id=None, is_main='yes', transport_brand_pts='Диагностическая карта', model='Мдл', ecoclass=TransportEcoClassFactory())
        trans_doc2 = TransportDocumentFactory(transport=[transport], types_of_document=self.type_doc5, serial_number=None, number=None,
                                 issue_date=None, date_end=None, issue_organization=None, vin_number=None, color=None,
                                 kuzov_number=None, doc_id=None, is_main='yes', transport_brand_pts='ПТС', model='Мдл')
        driver = DriverFactory()
        transport.drivers.set([driver])
        transport.participant = participant
        transport.type = transport_type
        transport.number = 'M755CY61'
        transport.save()
        trans_pass_request = TransportPassRequest.objects.filter(transport_pass_requests__in=transport.pass_requests.all()).first()
        try:
            trans_pass_request.send_to_ovga(trans_pass_request, self.transition1)
        except ValidationError:
            pass_request = trans_pass_request.transport_pass_requests.first()
            expected_phrase = content_repeat + 'в контактах не указан почтовый адрес.'
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = content_repeat + 'в контрагенте не указано полное наименование организации.'
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = content_repeat + 'в контрагенте не указан ИНН.'
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = content_repeat + 'в контрагенте не указан ОГРН.'
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = content_repeat + 'в контактах не указан телефонный номер.'
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = content_repeat + 'не указан государственный регистрационный знак.'
            self.assertFalse(expected_phrase in pass_request.body_message)
            expected_phrase = content_repeat + 'не указан класс транспорта.'
            self.assertFalse(expected_phrase in pass_request.body_message)
            expected_phrase = content_repeat + 'не указана марка транспорта.'
            self.assertFalse(expected_phrase in pass_request.body_message)
            expected_phrase = content_repeat + 'не указана модель транспорта.'
            self.assertFalse(expected_phrase in pass_request.body_message)
            expected_phrase = content_repeat + 'ни в одном приложенном документе не указан Экокласс.'
            self.assertFalse(expected_phrase in pass_request.body_message)
            expected_phrase = f"В документе транспорта с ID {trans_doc1.id} не указаны"
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = f"В документе транспорта с ID {trans_doc2.id} не указаны"
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = f"тип документа"
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = f"идентификатор ЦХЭД"
            self.assertTrue(expected_phrase in pass_request.body_message)
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = f"Необходимо указать документ с ВУ для водителя с ID {driver.id}."
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = f"Необходимо присвоить ФИО для водителя с ID {driver.id}."
            self.assertTrue(expected_phrase in pass_request.body_message)
        participant = ParticipantFactory(**self.participant_data)
        transport.participant = participant
        transport.save()
        driver.name = 'Иванов Иван Иванович'
        driver.save()
        driver_document = DriverDocumentFactory(**{'driver': driver, 'is_main': 'yes', 'types_of_document': None,
                                                   'series': None, 'number': None, 'date': None, 'period': None,
                                                   'issued_by': None, 'issued_by_code': None, 'doc_id': None})
        trans_doc1.delete()
        trans_doc2.delete()
        sign_status = DocStatusFactory(code='otpravlen-v-ched')
        trans_doc1 = TransportDocumentFactory(
            **{'transport': [transport], 'types_of_document': self.type_doc2, 'is_main': 'yes', 'status': sign_status,
               'transport_brand_pts': 'СТС', 'model': 'Мдл', 'ecoclass': TransportEcoClassFactory()})
        trans_doc2 = TransportDocumentFactory(
            **{'transport': [transport], 'types_of_document': self.type_doc6, 'is_main': 'yes', 'status': sign_status,
               'transport_brand_pts': 'ПТС', 'model': 'Мдл'})
        trans_doc3 = TransportDocumentFactory(
            **{'transport': [self.transport], 'types_of_document': self.type_doc5, 'is_main': 'yes',
               'status': sign_status, 'transport_brand_pts': 'ПТС', 'model': 'Мдл'})
        trans_pass_request = TransportPassRequest.objects.filter(
            transport_pass_requests__in=transport.pass_requests.all()).first()
        try:
            trans_pass_request.send_to_ovga(trans_pass_request, self.transition1)
        except ValidationError:
            pass_request = trans_pass_request.transport_pass_requests.first()
            expected_phrase = content_repeat + 'в контактах не указан почтовый адрес.'
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = content_repeat + 'в контрагенте не указано полное наименование организации.'
            self.assertFalse(expected_phrase in pass_request.body_message)
            expected_phrase = content_repeat + 'в контрагенте не указан ИНН.'
            self.assertFalse(expected_phrase in pass_request.body_message)
            expected_phrase = content_repeat + 'в контрагенте не указан ОГРН.'
            self.assertFalse(expected_phrase in pass_request.body_message)
            expected_phrase = content_repeat + 'в контактах не указан телефонный номер.'
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = f"В документе транспорта с ID {trans_doc1.id} не указаны"
            self.assertFalse(expected_phrase in pass_request.body_message)
            expected_phrase = f"В документе транспорта с ID {trans_doc2.id} не указаны"
            self.assertFalse(expected_phrase in pass_request.body_message)
            expected_phrase = f"В документе транспорта с ID {trans_doc3.id} не указаны"
            self.assertFalse(expected_phrase in pass_request.body_message)
            expected_phrase = f"тип документа"
            self.assertFalse(expected_phrase in pass_request.body_message)
            expected_phrase = f"идентификатор ЦХЭД"
            self.assertFalse(expected_phrase in pass_request.body_message)
            expected_phrase = f"номер документа"
            self.assertFalse(expected_phrase in pass_request.body_message)
            expected_phrase = f"дата начала"
            self.assertFalse(expected_phrase in pass_request.body_message)
            expected_phrase = f"дата окончания"
            self.assertFalse(expected_phrase in pass_request.body_message)
            expected_phrase = f"Необходимо присвоить ФИО для водителя с ID {driver.id}."
            self.assertFalse(expected_phrase in pass_request.body_message)
            expected_phrase = f'необходимо указать Номер ВУ для документа с ID {driver_document.id}'
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = f'необходимо присвоить Категорию ВУ для документа с ID {driver_document.id}'
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = f'не указан Тип документа для документа с ID: {driver_document.id}'
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = f'необходимо указать не указана дата выдача ВУ для документа с ID: {driver_document.id}'
            self.assertTrue(expected_phrase in pass_request.body_message)
            expected_phrase = f'необходимо отправить документ с ID: {driver_document.id} в ЦХЭД'
            self.assertTrue(expected_phrase in pass_request.body_message)
        self.set_contact(participant.id)
        driver_document.delete()
        driver_document = DriverDocumentFactory(
            **{'driver': driver, 'types_of_document': self.type_doc1, 'is_main': 'yes', 'status': sign_status, 'category': 'N,M'})
        trans_pass_request = TransportPassRequest.objects.filter(
            transport_pass_requests__in=transport.pass_requests.all()).first()
        try:
            trans_pass_request.send_to_ovga(trans_pass_request, self.transition1)
        except ValidationError:
            pass_request = trans_pass_request.transport_pass_requests.first()
            expected_phrase = 'отсутствуют разрешенные категории'
            self.assertTrue(expected_phrase in pass_request.body_message)
        driver_document.series = 'B,C1'
        driver_document.category = 'B,C1'
        driver_document.save()
        trans_pass_request.types_of_document = TypeDocumentFactory()
        trans_pass_request.save()
        result, status_code = trans_pass_request.send_to_ovga(trans_pass_request, self.transition1)
        self.assertTrue(result)
        self.assertRegex(result, self.uuid4_pattern)
        self.assertEqual(status_code, 200)
        is_send_request_ovga = send_request_ovga(TransportPassRequest.__name__, trans_pass_request.id,
                                                 {'key':'val'}, self.transition_status_1.id,
                                                 self.transition_status_to_1.id, self.user.id)
        self.assertTrue(is_send_request_ovga)
        trans_pass_request = TransportPassRequest.objects.filter(
            transport_pass_requests__in=transport.pass_requests.all()).first()
        pass_request = trans_pass_request.transport_pass_requests.first()
        self.assertEqual(pass_request.status, self.transition_status_to_1)
        self.assertEqual(trans_pass_request.status, self.transition_status_to_1)

    def test_create_annul_request(self):
        http_request = HttpRequest()
        http_request.GET = {'id_set': f'{self.pass_request.id}'}
        http_request.data = {'cancel_reason': 'Отмена'}
        http_request.user = self.user
        result, status_code = self.pass_request.annul_pass_requests(http_request)
        self.assertTrue(result)
        self.assertRegex(result, self.uuid4_pattern)
        self.assertEqual(status_code, 200)
        is_send_annul_ovga = send_annul_ovga([f'{self.pass_request.id}'], self.user.id, 'Отмена')
        self.assertTrue(is_send_annul_ovga)
        pass_request = PassRequests.objects.exclude(pass_date_cancellation=None)\
            .filter(pass_series_number=self.pass_request.pass_series_number, cancel_reason='Отмена',
                    pass_id=self.pass_request.pass_id).first()
        self.assertEqual(pass_request.pass_date_cancellation.date(), date.today())
        self.assertEqual(pass_request.cancel_reason, 'Отмена')
        self.assertEqual(pass_request.status.name, 'Отправлено на аннулирование')
        self.assertEqual(pass_request.request_status_send, 'SUCCESS')
        pass_request.pass_id = None
        pass_request.save()
        http_request.GET = {'id_set': f'{pass_request.id}'}
        with self.assertRaises(ValidationError):
            self.pass_request.annul_pass_requests(http_request)


class TransportPassRequestUpdateViewTests(TestCase):
    def __init__(self, methodName: str = ...):
        super().__init__(methodName)
        self.pass_request = None

    def setUp(self):
        self.client = APIClient()
        zone_data = {'name': 'МКАД', 'code': 'MKAD'}
        ZoneReferenceFactory(**zone_data)
        # Создаем объекты с помощью фабрик
        self.transport_pass_request = TransportPassRequestFactory()
        self.transport_pass_request.save()
        self.pass_request = PassRequestsFactory(trans_pass_request_id=self.transport_pass_request.id)
        self.pass_request.save()

    def test_update_transport_pass_request(self):
        from integrations.ovga_integration.serializers import FORMAT_DATE_FZ

        # Подготавливаем данные для обновления
        updated_data = {
            "requestId": self.pass_request.p_id,
            "requestStatus": "SUCCESS",
            "statusDate": "2023-09-22T14:30:00.000Z",
            "passId": uuid.uuid4(),
            "seriesNumber": "new_series_number",
            "status": "new_status",
            "dateCancellation": "2023-09-23T14:30:00.000Z",
            "cancelReason": "new_cancel_reason",
            "startDate": "2023-09-24T14:30:00.000Z",
            "endDate": "2023-09-25T14:30:00.000Z",
            "zone": "MKAD",  # Выберите значение из ZONE_CHOICES
            "reason": "new_reason",
        }

        # Отправляем запрос на обновление данных
        url = reverse('trans_pass_update')
        response = self.client.put(url, updated_data, format='json')

        # Проверяем, что запрос выполнен успешно (HTTP 200 OK)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Проверяем, что объекты были обновлены
        self.pass_request.refresh_from_db()

        self.assertEqual(self.pass_request.request_status, updated_data["requestStatus"])
        self.assertEqual(self.pass_request.status_date.replace(tzinfo=None),
                         datetime.strptime(updated_data["statusDate"], FORMAT_DATE_FZ))
        self.assertEqual(self.pass_request.pass_id, updated_data["passId"])
        self.assertEqual(self.pass_request.pass_series_number, updated_data["seriesNumber"])
        self.assertEqual(self.pass_request.pass_status, updated_data["status"])
        self.assertEqual(self.pass_request.pass_date_cancellation.replace(tzinfo=None),
                         datetime.strptime(updated_data["dateCancellation"], FORMAT_DATE_FZ))
        self.assertEqual(self.pass_request.cancel_reason, updated_data["cancelReason"])
        self.assertEqual(self.pass_request.start_date,
                         datetime.strptime(updated_data["startDate"], FORMAT_DATE_FZ).date())
        self.assertEqual(self.pass_request.end_date, datetime.strptime(updated_data["endDate"], FORMAT_DATE_FZ).date())
        self.assertEqual(self.pass_request.zone.code, updated_data["zone"])
        self.assertEqual(self.pass_request.reason, updated_data["reason"])

    def test_update_nonexistent_transport_pass_request(self):
        from integrations.ovga_integration.serializers import FORMAT_DATE_FZ
        # Создаем данные для запроса на обновление, но с несуществующим requestId
        non_existent_request_id = uuid.uuid4()
        updated_data = {
            "requestId": non_existent_request_id,
            "requestStatus": "SUCCESS",
            "statusDate": "2023-09-22T14:30:00.000Z",
        }

        # Отправляем запрос на обновление данных
        url = reverse('trans_pass_update')
        response = self.client.put(url, updated_data, format='json')

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        updated_data = {"zone": None, "passId": None, "reason": None, "status": None, "endDate": None,
                        "messageId": self.transport_pass_request.mess_id, "requestId": None, "startDate": None,
                        "statusDate": "2023-10-06T07:52:29.408Z", "cancelReason": None, "seriesNumber": None,
                        "requestStatus": "Доставлено в ОИВ", "dateCancellation": None}
        response = self.client.put(url, updated_data, format='json')
        # Проверяем, что запрос выполнен успешно (HTTP 200 OK)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Проверяем, что объекты были обновлены
        self.pass_request.refresh_from_db()
        self.assertEqual(self.pass_request.request_status, updated_data["requestStatus"])
        self.assertEqual(self.pass_request.status_date.replace(tzinfo=None),
                         datetime.strptime(updated_data["statusDate"], FORMAT_DATE_FZ))


class CheckChangeStatusTests(TestCase):
    def __init__(self, methodName: str = ...):
        super().__init__(methodName)
        self.transitions_list = []

    def setUp(self):
        # Создаем необходимые фабрики
        self.user = UserFactory(last_name='Иванов', first_name='Иван')
        self.uuid4_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
        self.transition_status_to_1 = DocStatusFactory(code='doc_status_to_1')
        self.transition_status_1 = DocStatusFactory(code='doc_status_1')
        self.transport_pass_request = TransportPassRequestFactory(status=self.transition_status_1)
        self.transport_pass_request.save()
        self.pass_request = PassRequestsFactory(trans_pass_request_id=self.transport_pass_request.id, status=self.transition_status_1)
        self.pass_request.save()
        self.transport_document1 = TransportDocumentFactory(status=self.transition_status_1)
        self.transport_document2 = TransportDocumentFactory(status=self.transition_status_to_1)
        self.driver_document = DriverDocumentFactory(status=self.transition_status_1)
        model_names = ['transportdocument', 'driverdocument', 'transportpassrequest', 'passrequests']
        self.role = RoleFactory()
        self.role.user.add(self.user)
        for model_name in model_names:
            table = ContentType.objects.filter(model=model_name).first()
            doc_type = DocTypeFactory(table=table, default_status=self.transition_status_1, code=f'doc_type_{model_name}')
            transition = DocStatusTransitionFactory(doc_type=doc_type, code='transition_code_1',
                                                    status=self.transition_status_1,
                                                    status_to=self.transition_status_to_1,
                                                    pre_transition='test_pre_transition',
                                                    post_transition='test_post_transition')
            transition_rule = DocStatusTransitionRulesFactory(transition=transition, role=self.role)
            transition_rule.role.user.add(self.user)
            self.transitions_list.append((transition.id, transition.doc_type.code))

    def test_change_status_task(self):
        model_name = 'transportdocument'
        id_set = {self.transport_document1.id, self.transport_document2.id}
        t_id = self.transitions_list[0][0]
        doc_type_code = self.transitions_list[0][1]
        req_user_id = self.user.id
        change_complete = change_stat_model_task(model_name, id_set, t_id, doc_type_code, req_user_id)
        self.assertTrue(change_complete)
        self.transport_document1.refresh_from_db()
        self.assertEqual(self.transport_document1.status_id, self.transition_status_to_1.id)
        self.assertEqual(self.transport_document2.status_id, self.transition_status_to_1.id)
        http_request = HttpRequest()

        http_request.GET = {'id_set': f'{self.transport_pass_request.id}'}
        http_request.data = {'e_doc_stat_transition': self.transitions_list[2][0]}
        http_request.user = self.user
        kwargs = {'model': 'transportpassrequest', 'doc_type': self.transitions_list[2][1]}
        result, status_res = self.transport_pass_request.change_status_trans_pass(http_request, **kwargs)
        self.assertRegex(result, self.uuid4_pattern)
        self.assertEqual(status_res, 200)

        http_request.GET = {'id_set': f'{self.pass_request.id}'}
        http_request.data = {'e_doc_stat_transition': self.transitions_list[3][0]}
        kwargs = {'model': 'passrequests', 'doc_type': self.transitions_list[3][1]}
        result, status_res = self.pass_request.change_status_pass(http_request, **kwargs)
        self.assertRegex(result, self.uuid4_pattern)
        self.assertEqual(status_res, 200)

        http_request.GET = {'id_set': f'{self.transport_document1.id}'}
        http_request.data = {'e_doc_stat_transition': self.transitions_list[0][0]}
        kwargs = {'model': 'transportdocument', 'doc_type': self.transitions_list[0][1]}
        result, status_res = self.transport_document1.change_status_trans_docs(http_request, **kwargs)
        self.assertRegex(result, self.uuid4_pattern)
        self.assertEqual(status_res, 200)

        http_request.GET = {'id_set': f'{self.driver_document.id}'}
        http_request.data = {'e_doc_stat_transition': self.transitions_list[1][0]}
        kwargs = {'model': 'driverdocument', 'doc_type': self.transitions_list[1][1]}
        result, status_res = self.driver_document.change_status_driver_docs(http_request, **kwargs)
        self.assertRegex(result, self.uuid4_pattern)
        self.assertEqual(status_res, 200)
