import logging
import requests
import re
from datetime import timedelta

from airflow import DAG, AirflowException
from airflow.operators.python_operator import PythonOperator

from helpers.utils import DataCleaningUtil
from helpers.mongo_utils import MongoOperations
from helpers.postgres_utils import PostgresOperations
from helpers.task_utils import notify, get_daily_start_date
from helpers.configs import (
    SURV_SERVER_NAME,
    SURV_USERNAME,
    SURV_PASSWORD,
    SURV_FORMS,
    SURV_DBMS,
    SURV_MONGO_DB_NAME,
    SURV_RECREATE_DB,
    SURV_MONGO_URI,
    POSTGRES_DB,
)

logger = logging.getLogger(__name__)

DAG_NAME = 'dots_survey_cto_data_pipeline'
PIPELINE = 'surveycto'

default_args = {
    'owner': 'Hikaya-Dots',
    'depends_on_past': False,
    'start_date': get_daily_start_date(),
    'catchup': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify(status='failed', pipeline=PIPELINE),
    'on_success_callback': notify(status='success', pipeline=PIPELINE)
}


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
            response = requests.get(data_url,
                                    auth=requests.auth.HTTPBasicAuth(
                                        SURV_USERNAME, SURV_PASSWORD))
        else:
            files = {'private_key': open(enc_key_file, 'rb')}
            response = requests.post(data_url,
                                     files=files,
                                     auth=requests.auth.HTTPBasicAuth(
                                         SURV_USERNAME, SURV_PASSWORD))

    except Exception as e:
        raise AirflowException(f'Fetch data failed with exception: {e}')

    response_data = response.json()

    return response_data


def get_form_url(form_id, last_date, status):

    form_url = f'https://{SURV_SERVER_NAME}.surveycto.com/api/v2/forms/data/wide/json/{form_id}?date={last_date}&r={status}'
    return form_url

def convert_surveycto_field(field):
    # TODO Refactor into its own module
    """
    Convert a single field of a survey to our format
    Detects and supports nested fields
    """
    if field['dataType'] == "group":
        print('Nested field')
        field['children'] = list(map(convert_surveycto_field, field['children']))
        return {
            'name': field['name'],
            'type': field['dataType'],
            'children': field['children']
        }
    else:
        return {
            'name': field['name'],
            'type': field['dataType']
        }

def get_forms():
    """
    List the IDs of the enabled and non-test forms
    """
    session = requests.Session()
    # Get CSRF token
    res = session.get(f'https://{SURV_SERVER_NAME}.surveycto.com/')
    print(res)
    csrf_token = re.search(r"var csrfToken = '(.+?)';", res.text).group(1)

    auth_basic = requests.auth.HTTPBasicAuth(SURV_USERNAME, SURV_PASSWORD)
    # TODO add a retry mechanism on this first request
    forms_request = session.get(
        f'https://{SURV_SERVER_NAME}.surveycto.com/console/forms-groups-datasets/get',
        auth=auth_basic,
        headers={
            "X-csrf-token": csrf_token,
            'X-OpenRosa-Version': '1.0',
            "Accept": "*/*"
        }
    )

    if forms_request.status_code != 200:
        print(forms_request.text)

    forms = forms_request.json()['forms']
    # ! Is this filter really working?
    forms = list(filter(lambda x: x['testForm'] == False and x['deployed'] == True, forms))

    forms_structures = []
    for form in forms:
        form_id = form['id']
        form_details = session.get(
            f'https://{SURV_SERVER_NAME}.surveycto.com/forms/{form_id}/workbook/export/load?includeFormStructureModel=true&submissionsPattern=all&fieldsPattern=all&fetchInBatches=true&includeDatasets=true&t=1612193019966',
            auth=auth_basic,
            headers={
                "X-csrf-token": csrf_token,
                'X-OpenRosa-Version': '1.0',
                "Accept": "*/*"
            }
        )
        if form_details.status_code == 200:
            form_structure_model = form_details.json()['formStructureModel']
            first_language = form_structure_model['defaultLanguage']
            fields = form_structure_model['summaryElementsPerLanguage'][first_language]['children']
            # Convert/transform fields to our format
            fields = list(map(convert_surveycto_field, fields))

            # TODO get the unique_colum from `submission_meta_data`
            forms_structures.append({
                'form_id': form['id'],
                'name': form['title'],
                'unique_column': 'KEY', # https://docs.surveycto.com/05-exporting-and-publishing-data/01-overview/09.data-format.html
                'fields': fields,
                'statuses': [],
                'last_date': form['lastIncomingDataDate'],
            })
        else:
            print(form_details.text)

    print('forms_structures')
    print(forms_structures)

    return forms

def get_form_data(form):
    """
    load form data from SurveyCTO API
    :param form:
    :return:
    """

    url = get_form_url(form.get('form_id', ''), form.get('last_date', 0),
                       '|'.join(form.get('statuses', ['approved', 'pending'])))

    response = fetch_data(url, form.get('keyfile'))
    response_data = response.json()

    logger.info('Get form data successful')

    return response_data


def save_data_to_db(**kwargs):
    """
    Depending on the specified DB save data
    :return:
    """
    get_forms()

    all_forms = len(SURV_FORMS)
    success_forms = 0
    for form in SURV_FORMS:
        # get columns
        columns = [item.get('name') for item in form.get('fields')]
        primary_key = form.get('unique_column')

        api_data = get_form_data(form)

        response_data = [
            DataCleaningUtil.clean_key_field(item, primary_key)
            for item in api_data
        ]

        if isinstance(response_data, (list, )) and len(response_data):

            if SURV_DBMS is not None and (SURV_DBMS.lower() == 'postgres'
                                          or SURV_DBMS.lower().replace(
                                              ' ', '') == 'postgresdb'):
                """
                Dump data to postgres 
                """

                # create the column strings
                column_data = [
                    PostgresOperations.construct_column_strings(
                        item, primary_key) for item in form.get('fields')
                ]

                # create the Db
                db_query = PostgresOperations.construct_postgres_create_table_query(
                    form.get('name'), column_data)

                connection = PostgresOperations.establish_postgres_connection(
                    POSTGRES_DB)

                with connection:
                    cur = connection.cursor()
                    if SURV_RECREATE_DB == 'True':
                        cur.execute("DROP TABLE IF EXISTS " + form.get('name'))
                        cur.execute(db_query)

                    # insert data
                    upsert_query = PostgresOperations.construct_postgres_upsert_query(
                        form.get('name'), columns, primary_key)

                    cur.executemany(
                        upsert_query,
                        DataCleaningUtil.update_row_columns(
                            form.get('fields'), response_data))
                    connection.commit()

                    success_forms += 1

            else:
                """
                Dump Data to MongoDB
                """
                mongo_connection = MongoOperations.establish_mongo_connection(
                    SURV_MONGO_URI, SURV_MONGO_DB_NAME)
                collection = mongo_connection[form.get('form_id')]
                mongo_operations = MongoOperations.construct_mongo_upsert_query(
                    response_data, primary_key)
                collection.bulk_write(mongo_operations)
                success_forms += 1

        else:
            logger.warn('The form {} has no data'.format(form.get('name')))

    if success_forms == all_forms:
        return dict(success=True)
    else:
        raise AirflowException('Not all forms data loaded')


def sync_db_with_server(**context):
    """
    Delete stale data from the DB and update any changed entries
    :param context:
    :return:
    """
    success_dump = context['ti'].xcom_pull(task_ids='Save_data_to_DB')

    if success_dump.get('success'):
        deleted_items = []
        deleted_data = []
        for form in SURV_FORMS:
            primary_key = form.get('unique_column')

            response_data = [
                DataCleaningUtil.clean_key_field(item, primary_key)
                for item in get_form_data(form)
            ]
            api_data_keys = [item.get(primary_key) for item in response_data]

            if SURV_DBMS is not None and (SURV_DBMS.lower() == 'postgres'
                                          or SURV_DBMS.lower().replace(
                                              ' ', '') == 'postgresdb'):
                """
                Delete data from postgres if DBMS is set to Postgres
                """
                connection = PostgresOperations.establish_postgres_connection(
                    POSTGRES_DB)
                db_data_keys = PostgresOperations.get_all_row_ids_in_db(
                    connection, primary_key, form.get('name'))

                deleted_ids = list(set(db_data_keys) - set(api_data_keys))
                if len(deleted_ids) > 0:
                    # remove deleted items from the db
                    query_string = PostgresOperations.construct_postgres_delete_query(
                        form.get('name'), primary_key, deleted_ids)

                    with connection:
                        cur = connection.cursor()

                        cur.execute(query_string)

                        connection.commit()

                        deleted_items.append(
                            dict(keys=deleted_ids,
                                 table=form.get('name'),
                                 number_of_items=len(deleted_ids)))

                    deleted_data.append(dict(success=deleted_items))
            else:
                """
                delete data from Mongo if DBMS is set to Mongo
                """
                mongo_connection = MongoOperations.establish_mongo_connection(
                    SURV_MONGO_URI, SURV_MONGO_DB_NAME)
                collection = mongo_connection[form.get('form_id')]
                ids_in_db = collection.distinct(primary_key)
                deleted_ids = list(set(ids_in_db) - set(api_data_keys))
                if len(deleted_ids) > 0:
                    collection.delete_many(
                        {'{}'.format(primary_key): {
                             '$in': deleted_ids
                         }})

        if len(deleted_data) > 0:
            return dict(report=deleted_data)
        else:
            return dict(message='All Data is up to date!!')

    else:
        raise AirflowException('Data dumping failed')


with DAG(DAG_NAME, default_args=default_args,
         schedule_interval='@daily') as dag:

    save_data_to_db_task = PythonOperator(
        task_id='Save_data_to_DB',
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

if __name__ == '__main__':
    SURV_USERNAME = "anastiour@gmail.com"
    SURV_PASSWORD = "R2d2c3po!"
    SURV_SERVER_NAME = "anastiour"
    SURV_FORMS="{}"
