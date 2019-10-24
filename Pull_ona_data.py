from airflow import DAG
from airflow.operators import BashOperator, PythonOperator
from datetime import datetime, timedelta
import requests
import logging
from pymongo import MongoClient

default_args = {
    'owner': 'Hikaya',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 1),
    'email': ['mail.nijinsha@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup_by_default': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('Pull_data_from_Ona', default_args=default_args)


def invoke_http(**kwargs):
    url = "https://api.ona.io/api/v1/projects"
    response = requests.get(url,
                headers= {'Authorization':'Token 4a9f8f198b2fb846fec2104271add15d3051a94d'}
                )
    return response.json()


def get_formdata(**context):
    ti = context['ti']
    json = ti.xcom_pull(task_ids='Get_projects_from_API')

    for el in json:
        survey_form = list(filter(lambda x: str(x['formid']) == '447910', el['forms']))
        if survey_form:
            url = "https://api.ona.io/api/v1/data/{}".format(survey_form[0]['formid'])
            response = requests.get(url,
                headers= {'Authorization':'Token 4a9f8f198b2fb846fec2104271add15d3051a94d'}
                )
            data = {'data': response.json(), 'project_name': el['name']}
    return data


def rename_keys(data):
    object_keys = data.keys()
    for item in object_keys:
        if item == 'S1metadata/donor':
            print('Found It::::: {}', str(item).replace('/', '_'))

        if '/' in str(item):
            new_name = str(item).replace('/', '_')
            data[new_name] = data[item]
            del data[item]
        else:
            pass
    return data


def clean_columns(row):
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


def insert_formdata(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='Get_formdata')
    client = MongoClient('mongodb://hikayaOna:Hikaya1244@167.71.38.240:27017/?serverSelectionTimeoutMS=5000&connectTimeoutMS=10000&authSource=ict_capacity&authMechanism=SCRAM-SHA-256&3t.uriVersion=3&3t.connection.name=ICT+Capacity')
    Hikaya = client['ict_capacity']
    collection = Hikaya[data['project_name']]
    logging.info('Data Size {} end'.format(len(data['data'])))

    for em in data['data']:
        formatted_data = clean_columns(em)
        '''
        use find_one_and_update with upsert=True
        to update if the submission already exists
        '''
        collection.find_one_and_update(
            {'ona_id': formatted_data['ona_id']},
            {'$set': formatted_data},
            upsert=True
        )


get_projects_from_api_task= PythonOperator(
    task_id='Get_projects_from_API',
    provide_context=True,
    python_callable=invoke_http,
    dag=dag,
)

get_formdata_task= PythonOperator(
    task_id='Get_formdata',
    provide_context=True,
    python_callable=get_formdata,
    dag=dag,
)

insert_formdata_task= PythonOperator(
    task_id='Insert_formsdata',
    provide_context=True,
    python_callable=insert_formdata,
    dag=dag,
)

get_projects_from_api_task>>get_formdata_task>>insert_formdata_task