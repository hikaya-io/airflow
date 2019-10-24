from airflow import DAG
from airflow.operators import BashOperator, PythonOperator
from datetime import datetime, timedelta
import requests
import logging
from pymongo import MongoClient

from airflow.models import Variable

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
ONA_API_URL = Variable.get('COMM_CARE_API_URL', default_var='')
ONA_TOKEN = Variable.get('COMM_CARE_TOKEN', default_var='')

dag = DAG('pull_data_from_comm_care', default_args=default_args)


# UTILITY METHODS
def clean_data_entries(entry):
    """
    clean data before saving to database
    :param entry: single submission
    :return cleaned_date:
    """
    pass


# MAIN TASKS METHODS
def get_comm_care_forms(**kwargs):
    """
    load CommCare forms
    :param kwargs:
    :return forms: dictionary list of forms
    """
    pass


def get_comm_care_form_data(**context):
    """
    load individual form data
    :param context:
    :return data: form submissions
    """
    pass


def save_comm_care_data_to_mongo_db(**context):
    """
    save form data to MongoDB
    :param context:
    :return:
    """
    pass


# TASKS
pull_comm_care_forms_task = PythonOperator(
    task_id='Pull_Comm_Care_Form_List',
    provide_context=True,
    python_callable=get_comm_care_forms,
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
