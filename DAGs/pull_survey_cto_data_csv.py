from datetime import timedelta
import logging
import re

import requests
import pandas

from airflow import DAG, AirflowException
from airflow.operators.python_operator import PythonOperator
from helpers.utils import logger
from helpers.postgres_utils import PostgresOperations
from helpers.task_utils import notify, get_daily_start_date
from helpers.configs import (
    SURV_SERVER_NAME,
    SURV_USERNAME,
    SURV_PASSWORD,
    POSTGRES_DB,
    POSTGRES_DB_PASSWORD,
    POSTGRES_USER,
    POSTGRES_HOST,
    POSTGRES_PORT,
)
from helpers.surveycto import SurveyCTO
from helpers.postgres_utils_sqlalchemy import PostgresOperations


logger = logging.getLogger(__name__)
DAG_NAME = "dots_survey_cto_data_csv_pipeline"
PIPELINE = "surveycto"
default_args = {
    "owner": "Hikaya-Dots",
    "depends_on_past": False,
    "start_date": get_daily_start_date(),
    "catchup": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify(status="failed", pipeline=PIPELINE),
    "on_success_callback": notify(status="success", pipeline=PIPELINE),
}


def import_forms_and_submissions(**kwargs):
    # TODO Keep track of which forms had exceptions on them
    # TODO handle MongoDB storage
    logger.info(f"Loading data from SurveyCTO server {SURV_SERVER_NAME}...")
    db = PostgresOperations(
        POSTGRES_USER,
        POSTGRES_DB_PASSWORD,
        POSTGRES_HOST,
        POSTGRES_PORT,
        POSTGRES_DB,
    ) # Its engine is needed by Pandas for saving into the DB

    scto_client = SurveyCTO(SURV_SERVER_NAME, SURV_USERNAME, SURV_PASSWORD)
    forms = scto_client.get_all_forms()
    logger.info(f"Found a total of {len(forms)} forms")

    for form in forms:
        try:
            submissions_dataframe = scto_client.get_form_submissions(form["id"])
            submissions_dataframe.to_sql(form["id"], db.engine, if_exists="replace")
            logger.info(f"Saved first-level submissions of form {form['id']}")

            form_details = scto_client.get_form(form["id"])
            form_structure_model = form_details.get("formStructureModel")
            first_language = form_structure_model.get("defaultLanguage")
            fields = form_structure_model["summaryElementsPerLanguage"][first_language][
                "children"
            ]
            repeat_groups = scto_client.get_repeat_groups(fields)

            for repeat_group in repeat_groups:
                dataframe = scto_client.get_repeat_group_submissions(form["id"], repeat_group["name"])
                dataframe.to_sql(form["id"] + "___" + repeat_group["name"], db.engine, if_exists="replace")
                logger.info(f"Saved repeat group {repeat_group['name']} of form {form['id']}")

            form_details = scto_client.get_form(form["id"])
        except:
            logger.error(f"Unexpected error handling form of ID: {form['id']}. Please see previous messages.")


with DAG(DAG_NAME, default_args=default_args, schedule_interval="@daily") as dag:

    dag.doc_md = """
    Load forms and their submissions from a SurveyCTO server.

    First, a list of all the forms is loaded.
    Then for each form, we get its details, including fields and their types.
    Then we get submissions of that form, then we get submissions of the repeat groups of
    that form separately.
    """
    t1 = PythonOperator(
        task_id="Save_data_to_DB",
        python_callable=import_forms_and_submissions,
        dag=dag,
    )
    t1
