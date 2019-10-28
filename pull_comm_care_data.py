from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models.variable import Variable

from datetime import datetime, timedelta
from pymongo import MongoClient
from pandas.io.json._normalize import nested_to_record

import requests
import logging
import json


default_args = {
    'owner': 'Hikaya',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 21),
    'email': ['odenypeter@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup_by_default': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# get all the variables
MONGO_DB_URI = Variable.get('MONGO_DB_URI', default_var='')
MONGO_DB_NAME = Variable.get('MONGO_DB_NAME', default_var='')
COMM_CARE_API_URL = Variable.get('COMM_CARE_API_URL', default_var='')
COMM_CARE_API_KEY = Variable.get('COMM_CARE_API_KEY', default_var='')
COMM_CARE_API_USERNAME = Variable.get('COMM_CARE_API_USERNAME', default_var='')

dag = DAG('pull_data_from_comm_care', default_args=default_args)


# UTILITY METHODS
def establish_db_connection(db_name):
    """
    establish MongoDB connection
    :return db_connection: database connection
    """
    client = MongoClient(MONGO_DB_URI)

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
        flat_data_item = nested_to_record(data_item, sep='_')

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
        if '.' in key or '$' in key or '/' in key:
            new_key = key.replace('.', '_').replace('$', '_').replace('/', '_')
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


# MAIN TASKS METHODS
def get_comm_care_form_data(**context):
    """
    load individual form data
    :param context:
    :return data: form submissions
    """
    ti = context['ti']
    forms_response_data = json.loads(ti.xcom_pull(task_ids='Get_Comm_Care_Forms'))
    print(forms_response_data)
    form_list = clean_form_list(forms_response_data['objects'])
    print(form_list)


def save_comm_care_data_to_mongo_db(**context):
    """
    save form data to MongoDB
    :param context:
    :return:
    """
    pass


# TASKS

pull_comm_care_forms_task = SimpleHttpOperator(
    task_id='Get_Comm_Care_Forms',
    method='GET',
    endpoint='form/',
    http_conn_id='comm_care_base_url',
    headers={"Content-Type":"application/json", "Authorization": "ApiKey {}:{}".format(
        COMM_CARE_API_USERNAME, COMM_CARE_API_KEY)},
    xcom_push=True,
    log_response=True,
    dag=dag,
)

pull_comm_care_form_data_task = PythonOperator(
    task_id='Pull_Comm_Care_Form_Data',
    provide_context=True,
    python_callable=get_comm_care_form_data,
    dag=dag,
)

save_comm_care_data_to_db_task = PythonOperator(
    task_id='Save_Comm_Care_Data_to_DB',
    provide_context=True,
    python_callable=save_comm_care_data_to_mongo_db,
    dag=dag,
)

# PIPELINE (WORKFLOW)
pull_comm_care_forms_task>>pull_comm_care_form_data_task>>save_comm_care_data_to_db_task
