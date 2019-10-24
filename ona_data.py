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
ONA_API_URL = Variable.get('ONA_API_URL', default_var='https://api.ona.io/api/v1/ ')
ONA_TOKEN = Variable.get('ONA_TOKEN', default_var='')

dag = DAG('pull_data_from_ona', default_args=default_args)


def get_ona_projects(**kwargs):
    """
    load ONA projects from ONA API
    """
    response = requests.get(
        '{}/projects'.format(ONA_API_URL),
        headers={'Authorization': 'Token {}'.format(ONA_TOKEN)}
    )
    return response.json()


def get_ona_form_data(**context):
    """
    get ONA form data
    :param context: from the previous task instance
    :return: form data
    """
    ti = context['ti']
    json = ti.xcom_pull(task_ids='Get_ONA_projects_from_API')

    for el in json:
        survey_form = list(filter(lambda x: str(x['formid']) == '447910', el['forms']))
        if survey_form:
            url = "{}data/{}".format(ONA_API_URL, survey_form[0]['formid'])
            response = requests.get(url, headers={'Authorization': 'Token {}'.format(ONA_TOKEN)})
            data = {'data': response.json(), 'project_name': el['name']}
    return data


def clean_form_data_columns(row):
    """
    rename columns to conform to Metabase expectations
    :param row: data coming from ONA API
    :return: new data object
    """
    new_object = {
        'ona_id': row['_id'] if '_id' in row else '',
        'uuid': row['_uuid'] if '_uuid' in row else '',
        'duration': row['_duration'] if '_duration' in row else '',
        'today': row['today'] if 'today' in row else '',
        'start_time': row['start_time'] if 'start_time' in row else '',
        'end_time': row['end_time'] if 'end_time' in row else '',

        'q_1': row['sectiona_background/q1'] if 'sectiona_background/q1' in row else '',
        'q_2': row['sectiona_background/q2'] if 'sectiona_background/q2' in row else '',
        'q_3': row['sectiona_background/q3'] if 'sectiona_background/q3' in row else '',
        'q_4': row['sectiona_background/q4'] if 'sectiona_background/q4' in row else '',
        'q_5': row['sectiona_background/q5'] if 'sectiona_background/q5' in row else '',
        'q_6': row['sectionb_ict_capacity/q6'] if 'sectionb_ict_capacity/q6' in row else '',
        'q_7': row['sectionb_ict_capacity/q7'] if 'sectionb_ict_capacity/q7' in row else '',
        'q_8': row['sectionb_ict_capacity/q8'] if 'sectionb_ict_capacity/q8' in row else '',

        'q_9a': row['sectionb_ict_capacity/q9/q9a'] if 'sectionb_ict_capacity/q9/q9a' in row else '',
        'q_9b': row['sectionb_ict_capacity/q9/q9b'] if 'sectionb_ict_capacity/q9/q9b' in row else '',
        'q_9c': row['sectionb_ict_capacity/q9/q9c'] if 'sectionb_ict_capacity/q9/q9c' in row else '',
        'q_9d': row['sectionb_ict_capacity/q9/q9d'] if 'sectionb_ict_capacity/q9/q9d' in row else '',
        'q_9e': row['sectionb_ict_capacity/q9/q9e'] if 'sectionb_ict_capacity/q9/q9e' in row else '',
        'q_9f': row['sectionb_ict_capacity/q9/q9f'] if 'sectionb_ict_capacity/q9/q9f' in row else '',
        'q_9g': row['sectionb_ict_capacity/q9/q9g'] if 'sectionb_ict_capacity/q9/q9g' in row else '',
        'q_9h': row['sectionb_ict_capacity/q9/q9h'] if 'sectionb_ict_capacity/q9/q9h' in row else '',

        'q_9h_specify_other': row['sectionb_ict_capacity/q9h_specify_other'] if 'sectionb_ict_capacity/q9h_specify_other' in row else '',
        'q_10': row['sectionb_ict_capacity/q10'] if 'sectionb_ict_capacity/q10' in row else '',
        'q_10_explain': row['sectionb_ict_capacity/q10_explain_why'] if 'sectionb_ict_capacity/q10_explain_why' in row else '',
        'q_11': row['sectiond_connectivity/q11'] if 'sectiond_connectivity/q11' in row else '',
        'q_12': row['sectiond_connectivity/q12'] if 'sectiond_connectivity/q12' in row else '',
        'q_13': row['sectiond_connectivity/q13'] if 'sectiond_connectivity/q13' in row else '',
        'q_14': row['sectiond_connectivity/q14'] if 'sectiond_connectivity/q14' in row else '',
        'q_15': row['sectiond_connectivity/q15'] if 'sectiond_connectivity/q15' in row else '',
        'q_16a': row['sectionc_it_services/q16/q16'] if 'ssectionc_it_services/q16/q16' in row else '',
        'q_16b': row['sectionc_it_services/q16/q16b'] if 'sectionc_it_services/q16/q16b' in row else '',
        'q_16c': row['sectionc_it_services/q16/q16c'] if 'sectionc_it_services/q16/q16c' in row else '',
        'q_16d': row['sectionc_it_services/q16/q16d'] if 'sectionc_it_services/q16/q16d' in row else '',
        'q_16e': row['sectionc_it_services/q16/q16e'] if 'sectionc_it_services/q16/q16e' in row else '',
        'q_16f': row['sectionc_it_services/q16/q16f'] if 'sectionc_it_services/q16/q16f' in row else '',
        'q_16g': row['sectionc_it_services/q16/q16g'] if 'sectionc_it_services/q16/q16g' in row else '',
        'q_16h': row['sectionc_it_services/q16/q16h'] if 'sectionc_it_services/q16/q16h' in row else '',

        'q_16i': row['sectionc_it_services/q16/q16i'] if 'sectionc_it_services/q16/q16i' in row else '',
        'q_16j': row['sectionc_it_services/q16/q16j'] if 'sectionc_it_services/q16/q16j' in row else '',
        'q_16k': row['sectionc_it_services/q16/q16k'] if 'sectionc_it_services/q16/q16k' in row else '',
        'q_6l': row['sectionc_it_services/q16/q16l'] if 'sectionc_it_services/q16/q16l' in row else '',
        'q_16m': row['sectionc_it_services/q16/q16m'] if 'sectionc_it_services/q16/q16m' in row else '',
        'q_16n': row['sectionc_it_services/q16/q16n'] if 'sectionc_it_services/q16/q16n' in row else '',
        'q_16o': row['sectionc_it_services/q16/q16o'] if 'sectionc_it_services/q16/q16o' in row else '',
        'q_16p': row['sectionc_it_services/q16/q16p'] if 'sectionc_it_services/q16/q16p' in row else '',

        'q_17': row['sectionc_it_services/q17'] if 'sectionc_it_services/q17' in row else '',
        'q_18': row['sectionc_it_services/q18'] if 'sectionc_it_services/q18' in row else '',
        'q_19': row['sectionc_it_services/q19'] if 'sectionc_it_services/q19' in row else '',
        'q_20': row['sectionc_it_services/q20'] if 'sectionc_it_services/q20' in row else '',
        'q_21': row['sectiong_ict_feedback/q21'] if 'sectiong_ict_feedback/q21' in row else '',
        'q_22': row['sectiong_ict_feedback/q22'] if 'sectiong_ict_feedback/q22' in row else '',


    }
    return new_object


def save_ona_data_to_mongo_db(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='Get_ONA_form_data')
    client = MongoClient(
        'mongodb://{}:{}@{}:{}/?serverSelectionTimeoutMS=5000&connectTimeoutMS=10000&authSource='
        'ict_capacity&authMechanism=SCRAM-SHA-256&3t.uriVersion=3&3t.connection.name=ICT+Capacity'.format(
            MONGO_DB_USER,
            MONGO_DB_PASSWORD,
            MONGO_DB_HOST,
            MONGO_DB_PORT
        ))
    db_client = client['ict_capacity']
    collection = db_client[data['project_name']]
    logging.info('Data Size {} end'.format(len(data['data'])))

    for em in data['data']:
        formatted_data = clean_form_data_columns(em)
        '''
        use find_one_and_update with upsert=True
        to update if the submission already exists
        '''
        collection.find_one_and_update(
            {'ona_id': formatted_data['ona_id']},
            {'$set': formatted_data},
            upsert=True
        )


# tasks
get_ONA_projects_from_api_task = PythonOperator(
    task_id='Get_ONA_projects_from_API',
    provide_context=True,
    python_callable=get_ona_projects,
    dag=dag,
)


get_ona_form_data_task = PythonOperator(
    task_id='Get_ONA_form_data',
    provide_context=True,
    python_callable=get_ona_form_data,
    dag=dag,
)

save_ONA_data_db_task = PythonOperator(
    task_id='Save_ONA_data_to_mongo_db',
    provide_context=True,
    python_callable=save_ona_data_to_mongo_db,
    dag=dag,
)

get_ONA_projects_from_api_task>>get_ona_form_data_task>>save_ONA_data_db_task
