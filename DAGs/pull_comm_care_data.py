from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models.variable import Variable

from datetime import datetime, timedelta
from pymongo import (MongoClient, UpdateOne)
from pandas.io.json._normalize import nested_to_record

import requests
import json

from helpers.configs import (
    COMM_CARE_MONGO_DB_URI,
    COMM_CARE_MONGO_DB_NAME,
    COMM_CARE_API_URL,
    COMM_CARE_API_KEY,
    COMM_CARE_API_USERNAME,
    COMM_CARE_PROGRAM_ID,
)


default_args = {
    'owner': 'Hikaya',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 31),
    'email': ['amos@hikaya.io'],
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup_by_default': False,
    'schedule_interval': timedelta(hours=4),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('pull_data_from_comm_care', default_args=default_args)

# set request headers and re-use
request_headers = {"Authorization": "ApiKey {}:{}".format(
    COMM_CARE_API_USERNAME, COMM_CARE_API_KEY)}


# UTILITY METHODS
def establish_db_connection(db_name):
    """
    establish MongoDB connection
    :return db_connection: database connection
    """
    client = MongoClient(COMM_CARE_MONGO_DB_URI)

    db_connection = client[db_name]
    return db_connection


def clean_form_list(forms):
    """
    extract the required information from forms response list
    :param forms: all forms
    :return clean forms:
    """
    clean_forms = []
    for item in forms:
        form = item['form']
        new_form_object = {
            'name': form['@name'],
            'data_url': form['@xmlns'],
            'enumerator': form['data_collector']['enumerator'],
            'enumerator_role': form['data_collector']['job_title'],
            'distribution_date': form['distribution_date'],
            'district': form['district'],
            'cash_amount':form['participant_group']['cash_amount'],
            'dist_type': form['participant_group']['dist_type'],
            'item_amount': form['participant_group']['item_amount'],
            'settlement': form['settlement'],
        }

        clean_forms.append(new_form_object)
    
    return clean_forms


def flatten_json_data(data):
    """
    flatten CommCare data (remove the json nesting)
    :param data: array of form submissions from
    :return clean:
    """
    flat_json_data = []

    field_list = None

    for data_item in data:
        # use pandas 'nested_to_record' method to flatten json
        # separate levels with '_'
        flat_data_item = nested_to_record(data_item['form'], sep='_')

        # clean the object keys
        clean_data_item = clean_object_keys(flat_data_item)

        # remove unwanted fields
        if field_list is not None:
            clean_data_item = clean_data_entries(clean_data_item, field_list)

        flat_json_data.append(clean_data_item)

    return flat_json_data


def clean_object_keys(clean_data_item):
    """
    clean object keys to remove illegal key characters for mongoDB
    :param clean_data_item:
    :return clean_data_item:
    """
    # use list for mutability

    for key, value in list(clean_data_item.items()):
        # remove illegal characters
        if '.' in key or '$' in key or '/' in key:
            new_key = key.replace('.', '_').replace('$', '_').replace('/', '_')
            clean_data_item[new_key] = value
            del clean_data_item[key]

        # remove unwanted characters on the keys ['@', '#']
        if '#' in key or '@' in key:
            new_key = key.replace('#', '').replace('@', '')
            clean_data_item[new_key] = value
            del clean_data_item[key]

    return clean_data_item


def clean_data_entries(entry, field_list=None):
    """
    keep only the wanted fields
    :param entry: single submission
    :param field_list: wanted field-list
    :return entry: entry dict with required fields only
    """
    for key, value in list(entry.items()):
        if key not in field_list:
            # delete field if not in field-list
            del entry[key]
    return entry


def save_data_to_mongo_db(clean_data, form_name):
    """
    save data to MongoDB
    :param clean_data:
    :param form_name:
    :return:
    """
    connection = establish_db_connection('uganda_country_program')
    document_collection = connection[form_name]

    # use bulk update MongoDB API to save the data
    unique_ids = [item.pop('meta_instanceID') for item in clean_data]
    operations = [
        UpdateOne(
            {'meta_instanceID': unique_id},
            {'$set': data}, upsert=True) for unique_id, data in zip(unique_ids, clean_data)]
    document_collection.bulk_write(operations)


def remove_deleted_submissions_from_db(ids_from_api, form_name):
    """
    remove from db, those items that have been deleted upstream
    :return:
    """
    db_connection = establish_db_connection('uganda_country_program')
    collection = db_connection[form_name]

    ids_in_db = collection.distinct('meta_instanceID')
    deleted_ids = list(set(ids_in_db) - set(ids_from_api))

    collection.delete_many({'meta_instanceID': {'$in': deleted_ids}})


# MAIN TASKS METHODS
def get_application_module_forms(**context):
    """
    extract the required information from applications response list
    :return clean forms:
    """
    application = json.loads(context['ti'].xcom_pull(
        task_ids="Get_Comm_Care_Application"))

    forms = []

    for app_module in application['modules']:
        for module_form in app_module['forms']:
            form = {
                'id': module_form['unique_id'],
                'xmlns': module_form['xmlns'],
                'name': module_form['name']['en']
            }

            forms.append(form)

    return forms


def get_forms_data(**context):
    """
    get forms data
    :param context:
    :return:
    """
    forms = context['ti'].xcom_pull(task_ids='Extract_Forms')

    for form in forms:
        response = None
        try:
            response = requests.get(
                '{}/form?xmlns={}&limit=1000'.format(COMM_CARE_API_URL, form['xmlns']), headers=request_headers)
        except requests.Timeout:
            pass

        if response is not None:
            form_data = response.json()

            actual_data = flatten_json_data(form_data['objects'])
            save_data_to_mongo_db(actual_data, form['name'])

            while form_data['meta']['next'] is not None:
                response = requests.get(
                    '{}/form{}'.format(COMM_CARE_API_URL, form_data['meta']['next']), headers=request_headers)
                form_data = response.json()
                actual_data = flatten_json_data(form_data['objects'])
                save_data_to_mongo_db(actual_data, form['name'])


def clean_stale_data_on_db(**context):
    """
    Reemove stale data from db
    :return:
    """
    forms = context['ti'].xcom_pull(task_ids='Extract_Forms')

    for form in forms:
        response = None
        try:
            response = requests.get(
                '{}/form?xmlns={}&limit=1000'.format(COMM_CARE_API_URL, form['xmlns']),
                headers=request_headers)
        except requests.Timeout:
            pass

        if response is not None:
            form_data = response.json()

            actual_data = flatten_json_data(form_data['objects'])

            instance_ids = [item.pop('meta_instanceID') for item in actual_data]

            while form_data['meta']['next'] is not None:
                response = requests.get(
                    '{}/form{}'.format(COMM_CARE_API_URL, form_data['meta']['next']),
                    headers=request_headers)
                form_data = response.json()
                actual_data = flatten_json_data(form_data['objects'])

                more_ids = [item.pop('meta_instanceID') for item in actual_data]
                new_list = list(set(instance_ids + more_ids))
                instance_ids = new_list

            remove_deleted_submissions_from_db(instance_ids, form['name'])


# TASKS
get_comm_care_application_task = SimpleHttpOperator(
    task_id='Get_Comm_Care_Application',
    method='GET',
    endpoint='/application/{}'.format(COMM_CARE_PROGRAM_ID),
    http_conn_id='comm_care_base_url',
    headers=request_headers,
    xcom_push=True,
    log_response=True,
    dag=dag,
)

extract_forms_task = PythonOperator(
    task_id="Extract_Forms",
    python_callable=get_application_module_forms,
    provide_context=True,
    dag=dag,
)

fetch_forms_data_task = PythonOperator(
    task_id="Fetch_Forms_Data",
    python_callable=get_forms_data,
    provide_context=True,
    dag=dag,
)

delete_stale_data_task = PythonOperator(
    task_id='Delete_Stale_Data',
    python_callable=clean_stale_data_on_db,
    provide_context=True,
    dag=dag,
)

# PIPELINES
get_comm_care_application_task >> extract_forms_task >> fetch_forms_data_task
extract_forms_task >> delete_stale_data_task
