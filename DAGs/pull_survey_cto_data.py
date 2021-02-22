# Delete me please
import logging
import requests
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from helpers.utils import (DataCleaningUtil, logger)
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
        logger.error('Fetching data from SurveyCTO failed')
        logger.exception(e)
        response_data = dict(success=False, error=e)

    return response_data


def get_form_url(form_id, last_date, status):

    form_url = f'https://{SURV_SERVER_NAME}.surveycto.com/api/v2/forms/data/wide/json/{form_id}?date={last_date}&r={status}'
    return form_url


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
    response_data = response.json()

    logger.info('Get form data successful')

    return response_data


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
            logger.error('The form {} has no data'.format(form.get('name')))

    if success_forms == all_forms:
        return dict(success=True)
    else:
        logger.error('Not all forms data loaded')
        return dict(failure='Not all forms data loaded')


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
