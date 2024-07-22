import datetime
import logging

import numpy as np
import pandas as pd
from django.core.exceptions import ValidationError
from django.db.models import Q

from file_upload.models import UploadedFile
from integrations.ovga_integration.models import TransportDocument, TypeDocument, DriverDocument, ZoneValues, get_id_set
from model_app.vehicles.models import Driver, Transport, TransportType, TransportBrand, TransportModel, \
    TransportEcoClass
from notification import constants as c
from statistic.clickhouse.connection import ch_client
import requests
from urllib.parse import urlencode

logger = logging.getLogger("b3.set_transport_document")
brand_name_str = 'brand.name'
trans_track_id = 'transport.tracker_id'
model_name_str = 'model.name'
drivers_str = 'Водители'
lizing_provider_str = 'Лизингодатель'
diagnostic_card_str = 'Диагностическая карта'


def load_trans_doc_files(name, link):
    """Загрузка файлов с "Яндекс диска" в облачное хранилище"""
    base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'

    # Получаем загрузочную ссылку
    final_url = base_url + urlencode(dict(public_key=link))
    response = requests.get(final_url)
    try:
        download_url = response.json()['href']
    except Exception:
        return None

    # Загружаем файл и сохраняем его
    download_response = requests.get(download_url)
    try:
        extension = download_response.headers._store['content-disposition'][1].split('.')[1]  # Пробуем словить расширение
        name = f'{name}.{extension}'
    except Exception:
        name = f'{name}.{datetime.datetime.today().strftime("%d.%m.%y.")}.pdf'
    up_file = UploadedFile.create(name, download_response.content)
    return up_file.id


def load_sts_pts(file, transport, document_transport, doc_status_id):
    logger.info('Получаем СТС')
    load_sts = pd.read_excel(file, sheet_name='СТС', header=1, dtype={'series': str, 'number': str, trans_track_id: str})
    load_sts = load_sts.astype({'series': 'str', 'number': 'str', trans_track_id: str})
    load_sts = load_sts.merge(transport[['tracker_id', 'transport_id']], how='left', right_on='tracker_id', left_on=trans_track_id, validate="many_to_many")
    load_sts['tracker_id'].fillna('', inplace=True)
    eco_class = pd.DataFrame(TransportEcoClass.objects.values('id', 'name'))
    eco_class.rename(columns={'id': 'ecoclass_id', 'name': 'ecoclass_name'}, inplace=True)
    load_sts = load_sts.merge(eco_class, how='left', left_on='eco_class', right_on='ecoclass_name', validate="many_to_many")
    load_sts.drop(columns=['tracker_id', 'eco_class', 'ecoclass_name'], inplace=True)
    load_sts.rename(columns={'vin': 'vin_number', 'series': 'serial_number', 'issued_date': 'issue_date',
                             'owner': 'issue_organization', model_name_str: 'model', 'type_text': 'type_of_transport',
                             'category_text': 'category', 'brand.name': 'transport_brand_pts'}, inplace=True)
    load_sts['issue_date'] = load_sts['issue_date'].dt.date
    doc_type_enum_id = ZoneValues.objects.get_or_create(code=10367, name='Паспорт транспортного средства')[0].id
    sts_type_id = TypeDocument.objects.get_or_create(type_document='СТС', doc_type_enum_id=doc_type_enum_id)[0].id
    load_sts['types_of_document_id'] = sts_type_id
    load_sts.drop(columns=[trans_track_id, 'year'], axis=1, inplace=True)
    if 'scan' in load_sts.columns:
        load_sts['scan_id'] = load_sts[['serial_number', 'number', 'scan']].apply(
            lambda x: load_trans_doc_files(f'{x[0]}_{x[1]}_', x[2]) if x[2] else None, axis=1)
        load_sts.drop(columns=['scan'], inplace=True)
    load_sts.fillna('', inplace=True)
    load_sts = load_sts.merge(document_transport[['id', 'types_of_document_id', 'serial_number', 'number']], how='left',
                              on=['types_of_document_id', 'serial_number', 'number'], validate="many_to_many")
    if 'color' not in load_sts.columns:
        load_sts['color'] = ''
    load_sts.fillna('', inplace=True)
    load_sts['status_id'] = doc_status_id
    load_sts['is_main'] = 'yes'
    load_sts_update = load_sts[load_sts['id'] != '']
    load_sts_update.drop(columns=['transport_id'], inplace=True)
    load_sts_create = load_sts[load_sts['id'] == '']
    load_sts_transport_create = load_sts_create[['types_of_document_id', 'serial_number', 'number', 'transport_id']].copy()
    load_sts_create.drop(columns=['id', 'transport_id'], inplace=True)

    logger.info('Получаем ПТС')
    load_pts = pd.read_excel(file, sheet_name='ПТС', header=1, dtype={'series': str, 'number': str, trans_track_id: str})
    load_pts = load_pts.merge(transport[['tracker_id', 'transport_id']], how='left', right_on='tracker_id', left_on=trans_track_id, validate="many_to_many")
    load_pts['issued_date'] = load_pts['issued_date'].astype('datetime64')
    pts_type_id = TypeDocument.objects.get_or_create(type_document='ПТС', doc_type_enum_id=doc_type_enum_id)[0].id
    load_pts['types_of_document_id'] = pts_type_id
    load_pts.rename(columns={'issued_date': 'issue_date', 'series': 'serial_number'}, inplace=True)
    if 'scan' in load_pts.columns:
        load_pts['scan_id'] = load_pts[['serial_number', 'number', 'scan']].apply(
            lambda x: load_trans_doc_files(f'{x[0]}_{x[1]}_', x[2]) if x[2] else None, axis=1)
        load_pts.drop(columns=['scan'], inplace=True)
    load_pts.rename(columns={'brand_text': 'transport_brand_pts'}, inplace=True)
    load_pts.drop(columns=[trans_track_id, 'tracker_id'], inplace=True)
    load_pts.fillna('', inplace=True)
    load_pts['status_id'] = doc_status_id
    load_pts['is_main'] = 'yes'
    load_pts = load_pts.merge(document_transport[['id', 'types_of_document_id', 'serial_number', 'number']], how='left',
                              on=['types_of_document_id', 'serial_number', 'number'], validate="many_to_many")
    if 'color' not in load_pts.columns:
        load_pts['color'] = ''
    load_pts.fillna('', inplace=True)
    load_pts_update = load_pts[load_pts['id'] != '']
    load_pts_update.drop(columns=['transport_id'], inplace=True)
    load_pts_create = load_pts[load_pts['id'] == '']
    load_pts_transport_create = load_pts_create[['types_of_document_id', 'serial_number', 'number', 'transport_id']].copy()
    load_pts_create.drop(columns=['id', 'transport_id'], inplace=True)

    logger.info('Готовим данные для загрузки')
    transport_document = []
    for lst in load_sts_update.to_dict('records'):
        transport_document.append(TransportDocument(**lst))
    for lpt in load_pts_update.to_dict('records'):
        transport_document.append(TransportDocument(**lpt))
    logger.info('Обновляем документы транспорта')
    TransportDocument.objects.bulk_update(transport_document,
                                          fields=['issue_date', 'issue_organization', 'vin_number', 'color', 'is_main',
                                                  'scan_id', 'status_id', 'max_weight', 'weight'], batch_size=2000)
    transport_document = []
    load_sts_create.drop_duplicates(subset=['types_of_document_id', 'serial_number', 'number'], inplace=True)
    for lst in load_sts_create.to_dict('records'):
        transport_document.append(TransportDocument(**lst))
    for lpt in load_pts_create.to_dict('records'):
        transport_document.append(TransportDocument(**lpt))
    logger.info('Загружаем документы транспорта')
    TransportDocument.objects.bulk_create(transport_document, batch_size=2000)
    numbers = np.concatenate([load_sts_create['number'].values, load_pts_create['number'].values])
    serial_numbers = np.concatenate([load_sts_create['serial_number'].values, load_pts_create['serial_number'].values])
    transport_document_loading = pd.DataFrame(TransportDocument.objects
                                              .filter(types_of_document_id__in=(sts_type_id, pts_type_id),
                                                      serial_number__in=serial_numbers, number__in=numbers)
                                              .values('id', 'types_of_document_id', 'serial_number', 'number'))
    transport_document_loading.rename(columns={'id': 'transportdocument_id'}, inplace=True)
    transport_document_db = TransportDocument.transport.through
    transport_document_db_list = []
    if not load_sts_transport_create.empty:
        load_sts_transport_create = load_sts_transport_create.merge(transport_document_loading, how='left',
                                                                    on=['types_of_document_id', 'serial_number', 'number'],
                                                                    validate="many_to_many")
        load_sts_transport_create.drop(columns=['types_of_document_id', 'serial_number', 'number'], inplace=True)
        for transport_document in load_sts_transport_create.to_dict('records'):
            transport_document_db_list.append(transport_document_db(**transport_document))
    if not load_pts_transport_create.empty:
        load_pts_transport_create = load_pts_transport_create.merge(transport_document_loading, how='left',
                                                                    on=['types_of_document_id', 'serial_number', 'number'],
                                                                    validate="many_to_many")
        load_pts_transport_create.drop(columns=['types_of_document_id', 'serial_number', 'number'], inplace=True)
        for transport_document in load_pts_transport_create.to_dict('records'):
            transport_document_db_list.append(transport_document_db(**transport_document))
    if transport_document_db_list:
        transport_document_db.objects.bulk_create(transport_document_db_list)
    return True


def load_diag_card(file, transport, document_transport, doc_status_id):
    logger.info('Получаем Диагностическую карту')
    load_diagnos_card = pd.read_excel(file, sheet_name=diagnostic_card_str, header=1, dtype={'number': str, trans_track_id: str})
    load_diagnos_card = load_diagnos_card.merge(transport[['tracker_id', 'transport_id']], how='left', right_on='tracker_id',
                                                left_on=trans_track_id, validate="many_to_many")
    load_diagnos_card.drop(columns=[trans_track_id, 'tracker_id'], inplace=True)
    load_diagnos_card['date_end'] = pd.to_datetime(load_diagnos_card['date_end']).dt.date
    doc_type_enum_id = ZoneValues.objects.get_or_create(code=10367, name='Паспорт транспортного средства')[0].id
    diagnos_type_id = TypeDocument.objects.get_or_create(type_document='Диагностическая карта',
                                                         doc_type_enum_id=doc_type_enum_id)[0].id
    load_diagnos_card['types_of_document_id'] = diagnos_type_id
    load_diagnos_card.fillna('', inplace=True)
    load_diagnos_card = load_diagnos_card.merge(document_transport[['number', 'id', 'types_of_document_id']],
                                                how='left', on=['number', 'types_of_document_id'], validate="many_to_many")
    load_diagnos_card.fillna('', inplace=True)
    load_diagnos_card['status_id'] = doc_status_id
    load_diagnos_card['is_main'] = 'yes'
    if 'scan' in load_diagnos_card.columns:
        load_diagnos_card['scan_id'] = load_diagnos_card[['number', 'types_of_document_id', 'scan']].apply(
            lambda x: load_trans_doc_files(f'{x[0]}_{x[1]}_', x[2]) if x[2] else None, axis=1)
        load_diagnos_card.drop(columns=['scan'], inplace=True)
    load_diagnos_card.fillna('', inplace=True)
    load_diagnos_card_update = load_diagnos_card[load_diagnos_card['id'] != '']
    load_diagnos_card_update.drop(columns=['transport_id'], inplace=True)
    load_diagnos_card_create = load_diagnos_card[load_diagnos_card['id'] == '']
    load_diagnos_card_transport_create = load_diagnos_card_create[['types_of_document_id', 'number', 'transport_id']].copy()
    load_diagnos_card_create.drop(columns=['id', 'transport_id'], inplace=True)
    logger.info('Обновляем Диагностические карты')
    transport_document = []
    for l_dcu in load_diagnos_card_update.to_dict('records'):
        transport_document.append(TransportDocument(**l_dcu))
    logger.info('Обновляем документы транспорта')
    TransportDocument.objects.bulk_update(transport_document, fields=['number', 'date_end', 'scan_id'],
                                          batch_size=2000)
    if not load_diagnos_card_create.empty:
        transport_document = []
        for l_dcu in load_diagnos_card_create.to_dict('records'):
            transport_document.append(TransportDocument(**l_dcu))
        logger.info('Создаем документы транспорта')
        TransportDocument.objects.bulk_create(transport_document, batch_size=2000)
        numbers = load_diagnos_card_create['number'].values
        transport_document_loading = pd.DataFrame(TransportDocument.objects
                                                  .filter(types_of_document_id=diagnos_type_id, number__in=numbers)
                                                  .values('id', 'types_of_document_id', 'number'))
        transport_document_loading.rename(columns={'id': 'transportdocument_id'}, inplace=True)
        load_diagnos_card_transport_create = load_diagnos_card_transport_create\
            .merge(transport_document_loading, how='left', on=['types_of_document_id', 'number'],
                   validate="many_to_many")
        load_diagnos_card_transport_create.drop(columns=['types_of_document_id', 'number'], inplace=True)
        transport_document_db = TransportDocument.transport.through
        transport_document_db_list = []
        for transport_document in load_diagnos_card_transport_create.to_dict('records'):
            transport_document_db_list.append(transport_document_db(**transport_document))
        transport_document_db.objects.bulk_create(transport_document_db_list)
    return True


def load_liz(file, transport, document_transport, doc_status_id):
    logger.info('Получаем Лизингодателя')
    load_lizing = pd.read_excel(file, sheet_name=lizing_provider_str, header=1, dtype={trans_track_id: str})
    load_lizing = load_lizing.merge(transport[['tracker_id', 'transport_id']], how='left', right_on='tracker_id',
                                    left_on=trans_track_id, validate="many_to_many")
    doc_type_enum_id = ZoneValues.objects.get_or_create(code=10901, name='Документ(ы), на основании которого(-ых) действует представитель заявителя')[0].id
    lizing_type_id = TypeDocument.objects.get_or_create(type_document='Лизингодатель',
                                                        doc_type_enum_id=doc_type_enum_id)[0].id
    load_lizing['types_of_document_id'] = lizing_type_id
    load_lizing.fillna('', inplace=True)
    load_lizing = load_lizing.astype({'tracker_id': 'str'})
    load_lizing.rename(columns={'name': 'issue_organization'}, inplace=True)
    load_lizing = load_lizing.merge(document_transport[['issue_organization', 'id', 'types_of_document_id', 'tracker_id']],
                                    how='left', on=['issue_organization', 'types_of_document_id', 'tracker_id'], validate="many_to_many")
    load_lizing.fillna('', inplace=True)
    if 'scan' in load_lizing.columns:
        load_lizing['scan_id'] = load_lizing[['issue_organization', 'tracker_id', 'scan']].apply(
            lambda x: load_trans_doc_files(f'{x[0]}_{x[1]}_', x[2]) if x[2] else None, axis=1)
        load_lizing.drop(columns=['scan'], inplace=True)
    load_lizing['number'] = load_lizing['tracker_id']
    load_lizing.drop(columns=[trans_track_id, 'tracker_id'], inplace=True)
    load_lizing['status_id'] = doc_status_id
    load_lizing['is_main'] = None
    load_lizing.fillna('', inplace=True)
    load_lizing = load_lizing.query('transport_id != ""')
    load_lizing_update = load_lizing[load_lizing['id'] != '']
    load_lizing_update.drop(columns=['transport_id'], inplace=True)
    load_lizing_create = load_lizing[load_lizing['id'] == '']
    load_lizing_transport_create = load_lizing_create[['types_of_document_id', 'issue_organization', 'number', 'transport_id']].copy()
    load_lizing_create.drop(columns=['id', 'transport_id'], inplace=True) if not load_lizing_create.empty else None
    transport_document = []
    for l_l in load_lizing_update.to_dict('records'):
        transport_document.append(TransportDocument(**l_l))
    TransportDocument.objects.bulk_update(transport_document,
                                          fields=['issue_organization', 'types_of_document_id', 'scan_id', 'is_main'],
                                          batch_size=2000)
    if not load_lizing_create.empty:
        transport_document = []
        for l_l in load_lizing_create.to_dict('records'):
            transport_document.append(TransportDocument(**l_l))
        TransportDocument.objects.bulk_create(transport_document, batch_size=2000)
        issue_organizations = load_lizing_create['issue_organization'].values
        numbers = load_lizing_create['number'].values
        transport_document_loading = pd.DataFrame(TransportDocument.objects
                                                  .filter(types_of_document_id=lizing_type_id, number__in=numbers,
                                                          issue_organization__in=issue_organizations)
                                                  .values('id', 'types_of_document_id', 'issue_organization', 'number'))
        transport_document_loading.rename(columns={'id': 'transportdocument_id'}, inplace=True)
        load_lizing_transport_create = load_lizing_transport_create \
            .merge(transport_document_loading, how='left', on=['types_of_document_id', 'issue_organization', 'number'],
                   validate="many_to_many")
        load_lizing_transport_create.drop(columns=['types_of_document_id', 'issue_organization', 'number'], inplace=True)
        transport_document_db = TransportDocument.transport.through
        transport_document_db_list = []
        for transport_document in load_lizing_transport_create.to_dict('records'):
            transport_document_db_list.append(transport_document_db(**transport_document))
        transport_document_db.objects.bulk_create(transport_document_db_list)
    return True


def get_driver_from_db():
    date_of_birth = pd.to_datetime('1900-01-01')
    drivers_db = pd.DataFrame(Driver.objects.values('id', 'name', 'birthday'))
    if drivers_db.empty:
        drivers_db = pd.DataFrame(columns=['id', 'name', 'birthday'])
    drivers_db['birthday'] = drivers_db['birthday'].fillna(date_of_birth)
    drivers_db.fillna('', inplace=True)
    drivers_db['birthday'] = pd.to_datetime(drivers_db['birthday'])
    drivers_db['birthday'] = drivers_db['birthday'].dt.date
    return drivers_db


def load_drivers(file, doc_status_id):
    logger.info('Получаем Водителей')
    drivers = pd.read_excel(file, sheet_name=drivers_str, header=1)
    drivers['name'] = drivers['last_name'] + ' ' + drivers['name'] + ' ' + drivers['second_name']
    drivers.drop(columns=['last_name', 'second_name'], inplace=True)
    drivers = drivers.astype({'series': 'str', 'number': 'str', trans_track_id: 'str'})
    drivers.rename(columns={trans_track_id: 'tracker_id', 'category.name': 'category', 'birthdate': 'birthday',
                            'issued_date': 'date'}, inplace=True)
    drivers.fillna('', inplace=True)
    drivers['category'] = drivers['category'].str.replace('А', 'A').str.replace('В', 'B').str.replace('С', 'C').str\
        .replace('Д', 'D').str.replace('Е', 'E').str.replace('М', 'M')
    drivers['birthday'] = drivers['birthday'].dt.date
    drivers.fillna('', inplace=True)
    drivers_db = get_driver_from_db()
    drivers = drivers.merge(drivers_db, how='left', on=['name', 'birthday'], validate="many_to_many")
    if 'scan' in drivers.columns:
        drivers['scan_id'] = drivers[['name', 'number', 'scan']].apply(
            lambda x: load_trans_doc_files(f'{x[0]}_{x[1]}_', x[2]) if x[2] else None, axis=1)
        drivers.drop(columns=['scan'], inplace=True)
    drivers.fillna('', inplace=True)
    drivers_documents = drivers.copy()
    drivers = drivers[['id', 'name', 'birthday']]
    drivers.drop_duplicates(subset=['name', 'birthday'], inplace=True)
    drivers_create = drivers[drivers['id'] == '']
    drivers_create.drop(columns='id', inplace=True)
    load = []
    for dr in drivers_create.to_dict('records'):
        load.append(Driver(**dr))
    Driver.objects.bulk_create(load, batch_size=2000)

    drivers_db = get_driver_from_db()
    drivers_documents.drop(columns=['id'], inplace=True)
    drivers_documents = drivers_documents.merge(drivers_db, how='left', on=['name', 'birthday'], validate="many_to_many")
    drivers_documents.rename(columns={'id': 'driver_id'}, inplace=True)
    transport = pd.DataFrame(Transport.objects.values('id', 'tracker_id'))
    transport = transport.query('not tracker_id.str.startswith("None.")', engine='python')
    transport['tracker_id'] = transport['tracker_id'].apply(lambda x: x if '.' in x else f'{x}.12345')
    transport[['tracker_id', 'port']] = transport.tracker_id.str.split(".", n=1, expand=True)
    transport.drop(columns=['port'], inplace=True)
    transport.rename(columns={'id': 'transport_id'}, inplace=True)
    transport = transport.merge(drivers_documents[['driver_id', 'tracker_id']], how='right', on='tracker_id')
    transport.drop(columns=['tracker_id'], inplace=True)
    transport['transport_id'].fillna(0, inplace=True)
    transport = transport.query('transport_id != 0')
    transport = transport.drop_duplicates()
    transport_driver_db = Transport.drivers.through
    transport_driver_db.objects.filter(transport_id__in=transport.transport_id.values.tolist()).delete()
    load = []
    for t_d in transport.to_dict('records'):
        load.append(transport_driver_db(**t_d))
    transport_driver_db.objects.bulk_create(load, batch_size=2000)
    drivers_documents.drop(columns=['tracker_id', 'name', 'birthday'], inplace=True)
    doc_type_enum_id = ZoneValues.objects.get_or_create(code=10901, name='Документ(ы), на основании которого(-ых) действует представитель заявителя')[0].id
    driver_vu = TypeDocument.objects.get_or_create(type_document='Водительское удостоверение', doc_type_enum_id=doc_type_enum_id)[0].id
    drivers_documents['types_of_document_id'] = driver_vu
    drivers_documents['is_main'] = 'yes'
    drivers_documents['status_id'] = doc_status_id
    drivers_docs_db = pd.DataFrame(DriverDocument.objects.values('id', 'types_of_document_id', 'number', 'driver_id'))
    if drivers_docs_db.empty:
        drivers_docs_db = pd.DataFrame(columns=['id', 'types_of_document_id', 'number', 'driver_id'])
    drivers_documents = drivers_documents.merge(drivers_docs_db, how='left', on=['types_of_document_id', 'number', 'driver_id'])
    drivers_documents.fillna('', inplace=True)
    drivers_documents.drop_duplicates(inplace=True)
    drivers_documents_update = drivers_documents[drivers_documents['id'] != '']
    drivers_documents_create = drivers_documents[drivers_documents['id'] == '']
    drivers_documents_create.drop(columns=['id'], inplace=True)
    load = []
    for dr_docs in drivers_documents_update.to_dict('records'):
        load.append(DriverDocument(**dr_docs))
    DriverDocument.objects.bulk_update(load, fields=['category', 'series', 'date', 'period', 'issued_by_code', 'scan_id'], batch_size=2000)
    load = []
    for dr_docs in drivers_documents_create.to_dict('records'):
        load.append(DriverDocument(**dr_docs))
    DriverDocument.objects.bulk_create(load, batch_size=2000)
    return True


def save_status(self):
    from workflow.models import DocStatus
    sending_data_status, _ = DocStatus.objects \
        .get_or_create(code='sending_data', defaults={'name': 'Запущена задача отправки данных'})
    self.status_id = sending_data_status.id
    self.save()


def send_doc_sign_ched(self, transition_obj, list_signs, file_id, asguf_code, document_class, mimetype, **kwargs):
    from integrations.ovga_integration.tasks import send_file_sign_ched
    save_status(self)
    task = send_file_sign_ched.delay(self.__class__.__name__, self.id, file_id, transition_obj.status_id,
                                     transition_obj.status_to_id, document_class, asguf_code, mimetype, list_signs,
                                     kwargs.get('user_id'))
    return task.id, 200


def create_request_ovga(self, json_request, transition_obj, **kwargs):
    from integrations.ovga_integration.tasks import send_request_ovga
    save_status(self)
    task = send_request_ovga.delay(self.__class__.__name__, self.id, json_request, transition_obj.status_id,
                                   transition_obj.status_to_id, kwargs.get('user_id'))
    return task.id, 200


def change_status_models(request, kwargs):
    if not request.data.get('e_doc_stat_transition'):
        raise ValidationError('Необходимо выбрать шаблон перехода!')
    from integrations.ovga_integration.tasks import change_stat_model_task
    model_name = kwargs.get('model')
    id_set = get_id_set(request.GET.get("id_set", ""))
    t_id = int(request.data.get('e_doc_stat_transition'))
    doc_type_code = kwargs.get('doc_type')
    task = change_stat_model_task.delay(model_name, id_set, t_id, doc_type_code, request.user.id)
    from integrations.ovga_integration.tasks import notice_send
    notice_send(None, request.user.id, True, f"Запущена процедура перехода статусов по ID: {id_set.__str__()}", theme=c.THEME_SHOW)
    return task.id, 200
