from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models.variable import Variable

from datetime import datetime, timedelta
from pymongo import MongoClient

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
MONGO_DB_USER = Variable.get('MONGO_DB_USER', default_var='')
MONGO_DB_PASSWORD = Variable.get('MONGO_DB_PASSWORD', default_var='')
MONGO_DB_HOST = Variable.get('MONGO_DB_HOST', default_var='127.0.0.1')
MONGO_DB_PORT = Variable.get('MONGO_DB_PORT', default_var=27017)
COMM_CARE_API_URL = Variable.get('COMM_CARE_API_URL', default_var='')
COMM_CARE_API_KEY = Variable.get('COMM_CARE_API_KEY', default_var='')
COMM_CARE_API_USERNAME = Variable.get('COMM_CARE_API_USERNAME', default_var='')
COMM_CARE_MONGO_AUTH_SOURCE = Variable.get('AUTH_SOURCE', default_var='')
COMM_CARE_MONGO_CONNECTION_NAME = Variable.get('CONNECTION_NAME', default_var='')

dag = DAG('pull_data_from_comm_care', default_args=default_args)


# UTILITY METHODS
def establish_db_connection(db_name):
    """
    establish MongoDB connection
    :return db_connection: database connection
    """
    client = MongoClient(
        'mongodb://{}:{}@{}:{}/?serverSelectionTimeoutMS=5000&connectTimeoutMS=10000&authSource='
        '{}&authMechanism=SCRAM-SHA-256&3t.uriVersion=3&3t.connection.name={}'.format(
            MONGO_DB_USER,
            MONGO_DB_PASSWORD,
            MONGO_DB_HOST,
            MONGO_DB_PORT,
            COMM_CARE_MONGO_AUTH_SOURCE,
            COMM_CARE_MONGO_CONNECTION_NAME
        )
    )

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


def clean_data_entries(entry):
    """
    clean data before saving to database
    :param entry: single submission
    :return cleaned_date:
    """
    pass


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
