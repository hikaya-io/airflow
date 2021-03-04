import logging
import requests
import re
from datetime import timedelta

from airflow import DAG, AirflowException
from airflow.operators.python_operator import PythonOperator
from helpers.utils import (DataCleaningUtil, logger)
from helpers.mongo_utils import MongoOperations
from helpers.postgres_utils import PostgresOperations
from helpers.requests import create_requests_session
from helpers.task_utils import notify, get_daily_start_date
from helpers.configs import (
    SURV_SERVER_NAME,
    SURV_USERNAME,
    SURV_PASSWORD,
    SURV_FORMS,
    SURV_DBMS,
    SURV_MONGO_DB_NAME,
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
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise AirflowException(f'Fetch data failed with exception: {e}')
    except Exception as e:
        raise AirflowException(f'Unexpected error when fetching data: {e}')

    return response.json()


def get_form_url(form_id, last_date, status):

    form_url = f'https://{SURV_SERVER_NAME}.surveycto.com/api/v2/forms/data/wide/json/{form_id}?date={last_date}&r={status}'
    return form_url


def get_forms():
    """
    List the schema and fields of the forms
    # TODO Flatten the fields
    # TODO convert fields types properly
    """
    session = create_requests_session(debug=False)
    # Get CSRF token
    try:
        res = session.get(f'https://{SURV_SERVER_NAME}.surveycto.com/')
        res.raise_for_status()
        csrf_token = re.search(r"var csrfToken = '(.+?)';", res.text).group(1)
    except requests.exceptions.HTTPError as e:
        logger.error(e)
        raise AirflowException('Couldn\'t load SurveyCTO landing page for getting the CSRF token')
    except requests.exceptions.RequestException as e:
        logger.error(e)
        raise AirflowException('Unexpected error loading SurveyCTO landing page')

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
        logger.error(forms_request.text)
        logger.error('Could not retrieve the list of forms')

    forms = forms_request.json()['forms']
    # ! Is this filter really working?
    forms = list(filter(lambda x: x['testForm'] == False and x['deployed'] == True, forms))

    forms_structures = []
    for form in forms:
        form_id = form.get('id')
        form_details = session.get(
            f'https://{SURV_SERVER_NAME}.surveycto.com/forms/{form_id}/workbook/export/load',
            params={
                'includeFormStructureModel': 'true',
                'submissionsPattern': 'all',
                'fieldsPattern': 'all',
                'fetchInBatches': 'true',
                'includeDatasets': 'false',
                'date': '1550011019966' # TODO set date conveniently
            },
            auth=auth_basic,
            headers={
                "X-csrf-token": csrf_token,
                'X-OpenRosa-Version': '1.0',
                "Accept": "*/*"
            }
        )
        if form_details.status_code == 200:
            form_structure_model = form_details.json().get('formStructureModel')
            first_language = form_structure_model.get('defaultLanguage')
            fields = form_structure_model['summaryElementsPerLanguage'][first_language]['children']

            # Convert/transform fields to our format
            # fields = list(map(convert_surveycto_field, fields))
            fields = [{
                'name': field.get('name'),
                'type': field.get('dataType')
            } for field in fields]
            # Adding the KEY__ field
            fields.append({
                'name': 'KEY',
                'type': 'text'
            })
            # Adding the CompletionDate/SubmissionDate datetime fields
            fields.append({
                'name': 'CompletionDate',
                'type': 'datetime'
            })
            fields.append({
                'name': 'SubmissionDate',
                'type': 'datetime'
            })
            # Adding other fields
            fields.append({
                'name': 'formdef_version',
                'type': 'text'
            })
            fields.append({
                'name': 'review_quality',
                'type': 'text'
            })
            fields.append({
                'name': 'review_status',
                'type': 'text'
            })

            forms_structures.append({
                'form_id': form.get('id'),
                'name': form.get('title'),
                'unique_column': 'KEY', # https://docs.surveycto.com/05-exporting-and-publishing-data/01-overview/09.data-format.html
                'fields': fields,
                'statuses': ['approved', 'pending'],
                # 'last_date': form.get('lastIncomingDataDate'), # TODO Should never be 0 or will cause API restrictions
                'last_date': 1549736155, # TODO Should never be 0 or will cause API restrictions
            })
        else:
            logger.error(form_details.text)
            logger.error(f'Could not retrieve details of the form {form_id}')

    return forms_structures


def get_form_data(form):
    """
    TODO properly handle invalid responses (e.g Bad Credentials)
    load form data from SurveyCTO API
    :param form:
    :return:
    """

    url = get_form_url(form.get('form_id', ''), form.get('last_date', 0),
                       '|'.join(form.get('statuses', ['approved', 'pending'])))
    response = fetch_data(url, form.get('keyfile'))
    logger.info('Get form data successful')

    return response


def save_data_to_db(**kwargs):
    """
    Save data to Postgres or MongoDB, depending on the variable SURV_DBMS
    :return:
    """
    total_success_forms = 0
    forms = get_forms()
    total_forms = len(forms)
    logger.info(f'Total number of forms in server is: {len(forms)}')

    for form in forms:
        # get columns
        if form.get('fields') is not None:
            columns = [item.get('name') for item in form.get('fields', [])]
        else:
            columns = []
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
                        item, primary_key) for item in (form.get('fields') or [])
                ]

                # create the Db
                db_query = PostgresOperations.construct_postgres_create_table_query(
                    form.get('form_id'), column_data)

                connection = PostgresOperations.establish_postgres_connection(
                    POSTGRES_DB)

                with connection:
                    cur = connection.cursor()

                    cur.execute("DROP TABLE IF EXISTS \"" + form.get('form_id') + "\"")
                    cur.execute(db_query)

                    # insert data
                    upsert_query = PostgresOperations.construct_postgres_upsert_query(
                        form.get('form_id'), columns, primary_key)

                    for submission in response_data:
                        # TODO Group all the fields by types and do transformations on them
                        integer_columns = [field.get('name') for field in form.get('fields') if field.get('type') == 'integer']
                        select_multiple_columns = [field.get('name') for field in form.get('fields') if field.get('type') == 'select_multiple']
                        # datetime_columns = [field.get('name') for field in form.get('fields') if field.get('type') in ['date', 'time', 'datetime']]
                        for col in integer_columns: # Set default and null values for integers in the submission
                            try:
                                if submission[col] == '':
                                    submission[col] = None # Empty string integer field defaults to None
                                else:
                                    submission[col] = int(submission[col])
                            except KeyError: # Column is missing from a submission
                                submission[col] = None
                            except ValueError:
                                logger.error('Field is not convertible to integer')
                        for col in select_multiple_columns:
                            try:
                                if submission[col] == '':
                                    submission[col] = [] # ? Should it?
                                # else: # TODO convert the column into an array
                            except KeyError:
                                submission[col] = None

                    if form.get('fields') is not None:
                        cur.executemany(
                            upsert_query,
                            DataCleaningUtil.update_row_columns(
                                form.get('fields'), response_data) # Need to Python type the response_data submissions
                        )
                        connection.commit()
                    total_success_forms += 1

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
                total_success_forms += 1

        else:
            total_success_forms += 1
            logger.warn('The form {} has no data'.format(form.get('name')))

    if total_success_forms == total_forms:
        logger.info(f'Loaded submissions of all the {total_forms} forms')
        return dict(success=True)
    else:
        logger.error(f'Only {total_success_forms} forms loaded out of a total of {total_forms} forms')
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

            if SURV_DBMS is not None and (
                SURV_DBMS.lower() == 'postgres' or
                SURV_DBMS.lower().replace(' ', '') == 'postgresdb'
            ):
                """
                Delete data from postgres if DBMS is set to Postgres
                """
                connection = PostgresOperations.establish_postgres_connection(
                    POSTGRES_DB)
                db_data_keys = PostgresOperations.get_all_row_ids_in_db(
                    connection, primary_key, form.get('form_id'))

                deleted_ids = list(set(db_data_keys) - set(api_data_keys))
                if len(deleted_ids) > 0:
                    # remove deleted items from the db
                    query_string = PostgresOperations.construct_postgres_delete_query(
                        form.get('form_id'), primary_key, deleted_ids)

                    with connection:
                        cur = connection.cursor()

                        cur.execute(query_string)

                        connection.commit()

                        deleted_items.append(
                            dict(keys=deleted_ids,
                                 table=form.get('form_id'),
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
            logger.info('Data has been deleted')
            return dict(report=deleted_data)
        else:
            logger.info('All Data is up to date')
            return dict(message='All Data is up to date!!')

    else:
        logger.error('Data dumping failed')
        return dict(failure='Data dumping failed')


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
