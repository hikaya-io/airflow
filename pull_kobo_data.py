from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models.variable import Variable

from datetime import datetime, timedelta

import requests


default_args = {
    'owner': 'Hikaya',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 15),
    'email': ['odenypeter@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup_by_default': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# get all the Kobo variables
KOBO_API_URL = Variable.get('KOBO_API_URL', default_var='')
KOBO_TOKEN = Variable.get('KOBO_TOKEN', default_var='')

# declare the dags
dag = DAG('pull_data_from_kobo', default_args=default_args)


def get_kobo_forms(**kwargs):
    """
    load Kobo forms from the API
    """
    response = requests.get(
        '{}/forms'.format(KOBO_API_URL),
        headers={'Authorization': 'Token {}'.format(KOBO_TOKEN)}
    )
    return response.json()


def get_kobo_form_data(**context):
    """
    get Kobo form data
    :param context: from the previous task instance
    :return: form data
    """
    ti = context.get('ti')
    json = ti.xcom_pull(task_ids='Get_Kobo_projects_from_API')

    for el in json:
        form = list(
            filter(lambda x: str(x.get('formid')) == '[form_id]', el.get('forms'))
        )
        if form:
            url = "{}data/{}".format(KOBO_API_URL, form[0].get('formid'))
            response = requests.get(url, headers={'Authorization': 'Token {}'.format(KOBO_TOKEN)})
            data = {'data': response.json(), 'project_name': el.get('name')}
    return data


# TASKS
get_kobo_projects_from_api_task = PythonOperator(
    task_id='Get_Kobo_forms_from_API',
    provide_context=True,
    python_callable=get_kobo_forms,
    dag=dag,
)


get_kobo_form_data_task = PythonOperator(
    task_id='Get_Kobo_form_data',
    provide_context=True,
    python_callable=get_kobo_form_data,
    dag=dag,
)
