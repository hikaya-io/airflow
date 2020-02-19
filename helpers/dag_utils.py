"""
DAG utility class
"""
from datetime import (datetime, timedelta,)

from helpers.configs import (
    DAG_EMAIL, DAG_EMAIL_ON_FAILURE, DAG_EMAIL_ON_RETRY,
    DAG_NO_OF_RETRIES, DAG_OWNER, DAG_RETRY_DELAY_IN_MINS,
)


class DagUtility:
    def __init__(self):
        pass

    @staticmethod
    def get_dag_default_args():
        return {
            'owner': DAG_OWNER,
            'depends_on_past': False,
            'start_date': datetime(2019, 10, 21),
            'email': [DAG_EMAIL],
            'email_on_failure': DAG_EMAIL_ON_FAILURE,
            'email_on_retry': DAG_EMAIL_ON_RETRY,
            'catchup_by_default': False,
            'retries': DAG_NO_OF_RETRIES,
            'retry_delay': timedelta(minutes=DAG_RETRY_DELAY_IN_MINS),
        }