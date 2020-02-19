from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

import requests

from helpers.utils import (DataCleaningUtil, )
from helpers.slack_utils import (SlackNotification,)
from helpers.mongo_utils import (MongoOperations,)
from helpers.postgres_utils import (PostgresOperations,)
from helpers.dag_utils import (DagUtility,)
from helpers.configs import (
    SURV_SERVER_NAME, SURV_USERNAME, SURV_PASSWORD, SURV_FORMS, SURV_DBMS,
    SURV_MONGO_DB_NAME, SURV_RECREATE_DB, SLACK_CONN_ID, SURV_MONGO_URI
)


dag = DAG(
    'hikaya_survey_cto_data_pipeline',
    default_args=DagUtility.get_dag_default_args()
)

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

        response_data = dict(success=False, error=e)

    return response_data


def get_form_url(form_id, last_date, status):

    form_url = f'https://{SURV_SERVER_NAME}.surveycto.com/api/v2/forms/data/wide/json/{form_id}?date={last_date}&r={status}'
    return form_url

def get_form_data(form):
    """
    load form data from SurveyCTO API
    :param form:
    :return:
    """
    if form.get('encrypted', False) is not False:

        # Let's pull form records for the encrypted form
        url = get_form_url(form.get('form_id', ''), form.get('last_date', 0),
            '|'.join(form.get('statuses', ['approved', 'pending'])))
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
        url = get_form_url(form.get('form_id', ''), form.get('last_date', 0),
            '|'.join(form.get('statuses', ['approved', 'pending'])))

        response = fetch_data(url)
        response_data = response.json()

    return  response_data


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
            DataCleaningUtil.clean_key_field(
                item,
                primary_key
            ) for item in api_data
        ]

        if isinstance(response_data, (list,)) and len(response_data):

            if SURV_DBMS is not None and (
                    SURV_DBMS.lower() == 'postgres' or
                    SURV_DBMS.lower().replace(' ', '') == 'postgresdb'
            ):
                """
                Dump data to postgres 
                """

                # create the column strings
                column_data = [
                    DataCleaningUtil.construct_column_strings(
                        item,
                        primary_key
                    ) for item in form.get('fields')
                ]

                # create the Db
                db_query = PostgresOperations.construct_postgres_create_table_query(
                    form.get('name'),
                    column_data
                )

                connection = PostgresOperations.establish_postgres_connection()

                with connection:
                    cur = connection.cursor()
                    if SURV_RECREATE_DB is True:
                        cur.execute("DROP TABLE IF EXISTS " + form.get('name'))
                        cur.execute(db_query)

                    # insert data
                    upsert_query = PostgresOperations.construct_postgres_upsert_query(
                        form.get('name'),
                        columns,
                        primary_key
                    )

                    cur.executemany(
                        upsert_query,
                        DataCleaningUtil.update_row_columns(
                            form.get('fields'),
                            response_data
                        )
                    )
                    connection.commit()

                    success_forms += 1


            else:
                """
                Dump Data to MongoDB
                """
                mongo_connection = MongoOperations.establish_mongo_connection(
                    SURV_MONGO_URI,
                    SURV_MONGO_DB_NAME
                )
                collection = mongo_connection[form.get('form_id')]
                mongo_operations = MongoOperations.construct_mongo_upsert_query(
                    response_data,
                    primary_key
                )
                collection.bulk_write(mongo_operations)
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
    success_dump = context['ti'].xcom_pull(task_ids='Save_data_to_DB')

    if success_dump.get('success', None) is not None:
        deleted_items = []
        deleted_data = []
        for form in SURV_FORMS:
            primary_key = form.get('unique_column')

            response_data = [
                DataCleaningUtil.clean_key_field(
                    item,
                    primary_key
                ) for item in get_form_data(form)
            ]
            api_data_keys = [
                item.get(primary_key) for item in response_data
            ]

            if SURV_DBMS is not None and (
                    SURV_DBMS.lower() == 'postgres' or
                    SURV_DBMS.lower().replace(' ', '') == 'postgresdb'
            ):
                """
                Delete data from postgres id DBMS is set to Postgres
                """
                connection = PostgresOperations.establish_postgres_connection()
                db_data_keys = PostgresOperations.get_all_row_ids_in_db(
                    connection,
                    primary_key,
                    form.get('name')
                )

                deleted_ids = list(set(db_data_keys) - set(api_data_keys))
                if len(deleted_ids) > 0:
                    # remove deleted items from the db
                    query_string = PostgresOperations.construct_postgres_delete_query(
                        form.get('name'),
                        primary_key,
                        deleted_ids
                    )

                    with connection:
                        cur = connection.cursor()

                        cur.execute(query_string)

                        connection.commit()

                        deleted_items.append(
                            dict(
                                keys=deleted_ids,
                                table=form.get('name'),
                                number_of_items=len(deleted_ids)
                            )
                        )

                    deleted_data.append(dict(success=deleted_items))
            else:
                """
                delete data from Mongo if DBMS is seto Mongo
                """
                mongo_connection = MongoOperations.establish_mongo_connection(
                    SURV_MONGO_URI,
                    SURV_MONGO_DB_NAME
                )
                collection = mongo_connection[form.get('form_id')]
                ids_in_db = collection.distinct(primary_key)
                deleted_ids = list(set(ids_in_db) - set(api_data_keys))
                if len(deleted_ids) > 0:
                    collection.delete_many({'{}'.format(primary_key): {'$in': deleted_ids}})

        if len(deleted_data) > 0:
            return dict(report=deleted_data)
        else:
            return dict(message='All Data is up to date!!')

    else:
        return dict(failure='Data dumping failed')


def task_success_slack_notification(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    attachments = SlackNotification.construct_slack_message(context, 'success')

    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        attachments=attachments,
        username='airflow'
    )
    return failed_alert.execute(context=context)


def task_failed_slack_notification(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    attachments = SlackNotification.construct_slack_message(context, 'failed')

    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        attachments=attachments,
        username='airflow')
    return failed_alert.execute(context=context)

# TASKS
save_data_to_db_task = PythonOperator(
    task_id='Save_data_to_DB',
    provide_context=True,
    python_callable=save_data_to_db,
    on_failure_callback=task_failed_slack_notification,
    on_success_callback=task_success_slack_notification,
    dag=dag,
)

sync_db_with_server_task = PythonOperator(
    task_id='Sync_DB_With_Server',
    provide_context=True,
    python_callable=sync_db_with_server,
    on_failure_callback=task_failed_slack_notification,
    on_success_callback=task_success_slack_notification,
    dag=dag,
)

save_data_to_db_task >> sync_db_with_server_task