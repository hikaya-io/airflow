from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.models.variable import Variable
from airflow.models.connection import Connection

from datetime import datetime, timedelta
from pymongo import MongoClient
from pandas.io.json._normalize import nested_to_record

import requests
import logging
import json
import requests


default_args = {
    'owner': 'Hikaya',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 21),
    'email': ['odenypeter@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup_by_default': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# get all the variables
MONGO_DB_USER = Variable.get('MONGO_DB_USER', default_var='')
MONGO_DB_PASSWORD = Variable.get('MONGO_DB_PASSWORD', default_var='')
MONGO_DB_HOST = Variable.get('MONGO_DB_HOST', default_var='127.0.0.1')
MONGO_DB_PORT = Variable.get('MONGO_DB_PORT', default_var=27017)
COMM_CARE_API_URL = Variable.get('COMM_CARE_API_URL', default_var='')
COMM_CARE_API_KEY = Variable.get('COMM_CARE_API_KEY', default_var='')
COMM_CARE_API_USERNAME = Variable.get('COMM_CARE_API_USERNAME', default_var='')
COMM_CARE_MONGO_AUTH_SOURCE = Variable.get('AUTH_SOURCE', default_var='')
COMM_CARE_MONGO_CONNECTION_NAME = Variable.get(
    'CONNECTION_NAME', default_var='')
COMM_CARE_UGANDA_COUNTRY_PROGRAM_ID = '96db17f55a000e019f7774d06880e623'

dag = DAG('commcare_uganda_country_program', default_args=default_args)
request_headers = {"Authorization": "ApiKey {}:{}".format(
    COMM_CARE_API_USERNAME, COMM_CARE_API_KEY)}


def establish_db_connection(db_name):
    """
    establish MongoDB connection
    :return db_connection: database connection
    """
    pass


def get_application_module_forms(**context):
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
    forms = context['ti'].xcom_pull(task_ids='Extract_Forms')

    for form in forms:
        response = requests.get(
            '{}/form?xmlns={}&limit=20'.format(COMM_CARE_API_URL, form['xmlns']), headers=request_headers)

        form_data = response.json()

        actual_data = clean_json_data(form_data['objects'])
        save_data(actual_data, form['name'])

        while form_data['meta']['next'] is not None:
            response = requests.get(
                '{}/form{}'.format(COMM_CARE_API_URL, form_data['meta']['next']), headers=request_headers)
            form_data = response.json()
            actual_data = clean_json_data(form_data['objects'])
            save_data(actual_data, form['name'])


def clean_json_data(data):
    clean_data = []

    for data_item in data:
        clean_data_item = nested_to_record(data_item, sep='_')

        clean_data_item = clean_object_keys(clean_data_item)

        clean_data.append(clean_data_item)

    return clean_data


def clean_object_keys(clean_data_item):
    data_keys = clean_data_item.keys()

    for key, value in list(clean_data_item.items()):
        if '.' in key or '$' in key or '/' in key:
            new_key = key.replace('.', '_').replace('$', '_').replace('/', '_')
            clean_data_item[new_key] = value
            del clean_data_item[key]
    return clean_data_item


def save_data(clean_data, form_name):
    connection = establish_db_connection('uganda_country_program')
    document_collection = connection[form_name]

    document_collection.insert_many(clean_data)


# TASKS
get_comm_care_application_task = SimpleHttpOperator(
    task_id='Get_Comm_Care_Application',
    method='GET',
    endpoint='/application/{}'.format(COMM_CARE_UGANDA_COUNTRY_PROGRAM_ID),
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

# PIPELINE
get_comm_care_application_task >> extract_forms_task >> fetch_forms_data_task
