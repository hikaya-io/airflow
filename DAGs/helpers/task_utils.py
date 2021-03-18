import logging
from datetime import datetime, timedelta, time

from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook

from helpers.slack_utils import SlackNotification

logger = logging.getLogger(__name__)


def notify(status, pipeline, alert=True, connection='slack'):
    """
    callback function used to alert of task failures and successes
    """

    def callback(context):
        slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
        task_id = context.get('task_instance').task_id
        dag_id = context.get('task_instance').dag_id
        if alert:
            attachments = SlackNotification.construct_slack_message(
                context=context, status=status, pipeline=pipeline)
            slack_hook = SlackWebhookOperator(
                http_conn_id=connection,
                task_id=task_id,
                webhook_token=slack_webhook_token,
                attachments=attachments
            )

            slack_hook.post_webhook(attachments)
        elif not alert and status == 'failed':
            logger.warning(
                f'DAG failure notification for task {task_id} in DAG {dag_id}')
        else:
            logger.info(f'DAG: {dag_id}, task: {task_id}, status: {status}')

    return callback


def get_daily_start_date(days=-1, hour=0, min=0):
    """
    set start_date to most recent occurrence
    """

    dt = datetime.utcnow()
    desired_start_date = dt + timedelta(days=days)

    return datetime.combine(desired_start_date, time(hour, min))
