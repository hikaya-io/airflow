from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import requests
from datetime import (datetime, timedelta,)

from helpers.utils import (
    construct_column_strings, construct_postgres_create_table_query,
    establish_postgres_connection, construct_postgres_upsert_query,
    update_row_columns,
)
from helpers.configs import (
    SURV_SERVER_NAME, SURV_USERNAME, SURV_PASSWORD, SURV_FORMS,
)

default_args = {
    'owner': 'Hikaya',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 21),
    'email': ['odenypeter@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup_by_default': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=6),
}


dag = DAG('hikaya_survey_cto_data_pipeline', default_args=default_args)

# def clean_survey_cto_form(form_fields_object):
#     rteurn


def fetch_data(data_url, enc_key_file=None):
    """
    Extract records or media files from the SurveyCTO Rest API depending on the url provided
    Include the keyfile parameter if your form or media file needs to be decrypted
    :param data_url: form url
    :param enc_key_file: SurveyCTO encryption key file
    :return:
    """

    try:

        if enc_key_file is None:
            response_data = requests.get(
                data_url,
                auth=requests.auth.HTTPDigestAuth(SURV_USERNAME, SURV_PASSWORD))
        else:
            files = {'private_key': open(enc_key_file, 'rb')}
            response_data = requests.post(
                data_url,
                files=files,
                auth=requests.auth.HTTPDigestAuth(SURV_USERNAME, SURV_PASSWORD))

    except Exception as e:

        response_data = False
        print(e)

    return response_data


def get_form_url(form_id, last_date, status):

    form_url = f'https://{SURV_SERVER_NAME}.surveycto.com/api/v2/forms/data/wide/json/{form_id}?date={last_date}&r={status}'
    return form_url


def save_data_to_db(**kwargs):
    """
    Depending on the specified DB save data
    :return:
    """
    all_forms = len(SURV_FORMS)
    success_forms = 0
    for form in SURV_FORMS:
        # get columns
        columns = [item.get('name') for item in form.get('fields')]
        primary_key = form.get('unique_column')

        if form.get('encrypted', False) is not  False:

            # Let's pull form records for the encrypted form
            url = get_form_url(
                form.get('form_id', ''),
                form.get('last_date', 0),
                '|'.join(form.get('statuses', ['approved', 'pending']))
            )
            """
            With the encryption key, we expect to see all the fields included in the response.
            If we don't include the encryption key the API will only return the unencrypted fields.
            """
            response = fetch_data(url, form.get('keyfile', None))
            response_data = response.json()

        else:
            """
            Pulling data for the unencrypted form will be the exact same except we don't
            provide a keyfile for the pull_data function
            """
            url = get_form_url(
                form.get('form_id', ''),
                form.get('last_date', 0),
                '|'.join(form.get('statuses', ['approved' ,'pending']))
            )

            response = fetch_data(url)
            response_data = response.json()

        if isinstance(response_data, (list,)) and len(response_data):
            # create the column strings
            column_data = [construct_column_strings(item) for item in form.get('fields')]

            # create the Db
            db_query = construct_postgres_create_table_query(form.get('name'), column_data)

            connection = establish_postgres_connection()

            with connection:
                cur = connection.cursor()
                cur.execute("DROP TABLE IF EXISTS " + form.get('name'))
                cur.execute(db_query)

                # insert data
                upsert_query = construct_postgres_upsert_query(
                    form.get('name'), columns, primary_key
                )

                cur.executemany(upsert_query, update_row_columns(form.get('fields'),response_data))
                connection.commit()

                success_forms += 1

        else:
            print(dict(message='The form {} has no data'.format(form.get('name'))))

    if success_forms == all_forms:
        return dict(success=True)
    else:
        return dict(failure='Not all forms data loaded')


def sync_db_with_server(**context):
    """
    Delete stale data from the DB and update any changed entries
    :param context:
    :return:
    """
    pass


# TASKS
save_data_to_db_task = PythonOperator(
    task_id='Save_data_to_DB',
    provide_context=True,
    python_callable=save_data_to_db,
    dag=dag,
)

sync_db_with_server_task = PythonOperator(
    task_id='Sync_DB_With_Server',
    provide_context=True,
    python_callable=sync_db_with_server,
    dag=dag,
)

save_data_to_db_task >> sync_db_with_server_task