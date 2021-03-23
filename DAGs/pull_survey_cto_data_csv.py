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
    SURV_DBMS,
    SURV_MONGO_DB_NAME,
    SURV_MONGO_URI,
    POSTGRES_DB,
)

logger = logging.getLogger(__name__)

DAG_NAME = 'dots_survey_cto_data_csv_pipeline'
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

def get_forms():
    print('a')

def import_form_submissions(form):
    """
    CSV SCTO import documentation: https://docs.surveycto.com/05-exporting-and-publishing-data/01-overview/09.data-format.html
    """
    # CSV direct
    csv_url = f"https://{SURV_SERVER_NAME}.surveycto.com/api/v1/forms/data/csv/{form.get('id')}"
    auth_basic = requests.auth.HTTPBasicAuth(SURV_USERNAME, SURV_PASSWORD)
    test = requests.get(
        csv_url,
        auth=auth_basic
    )

    # forms_request = session.get(
    #     f'https://{SURV_SERVER_NAME}.surveycto.com/console/forms-groups-datasets/get',
    #     auth=auth_basic,
    #     headers={
    #         "X-csrf-token": csrf_token,
    #         'X-OpenRosa-Version': '1.0',
    #         "Accept": "*/*"
    #     }
    # )
    # form_details = session.get(
    #     f'https://{SURV_SERVER_NAME}.surveycto.com/forms/{form_id}/workbook/export/load',
    #     params={
    #         'includeFormStructureModel': 'true',
    #         'submissionsPattern': 'all',
    #         'fieldsPattern': 'all',
    #         'fetchInBatches': 'true',
    #         'includeDatasets': 'false',
    #         'date': '1550011019966' # TODO set date conveniently
    #     },
    #     auth=auth_basic,
    #     headers={
    #         "X-csrf-token": csrf_token,
    #         'X-OpenRosa-Version': '1.0',
    #         "Accept": "*/*"
    #     }
    # )

    from io import StringIO
    import pandas

    string_io = StringIO(test.text)
    df = pandas.read_csv(string_io)
    print(df.shape)
    print(df.head(10))
    print(df.info())
    print(df.describe())

def import_forms_and_submissions(**kwargs):
    import_form_submissions({
        'id': 'airflow_sample_form'
    })

with DAG(DAG_NAME, default_args=default_args,
         schedule_interval='@daily') as dag:

    dag.doc_md = """
    Doc
    """
    t1 = PythonOperator(
        task_id='Save_data_to_DB',
        python_callable=import_forms_and_submissions,
        dag=dag,
    )

    # t2 = PythonOperator(
    #     task_id='Save_data_to_DB',
    #     python_callable=import_forms_and_submissions,
    #     dag=dag,
    # )

    # save_data_to_db_task >> sync_db_with_server_task
    # t1 >> t2
    t1
