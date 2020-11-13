import logging
from datetime import datetime, timedelta, time

from airflow.hooks.slack import SlackWebHook

from helpers.slack_utils import SlackNotification

logger = logging.getLogger(__name__)


def notify(status, pipeline, alert=True, connection='slack'):
    """
    callback function used to alert of task failures and successes
    """

    def callback(context):
        task_id = context.get('task_instance').task_id
        dag_id = context.get('task_instance').dag_id
        if alert:
            slack_hook = SlackWebHook(
                connection
            )  # TODO: Store connection name in Airflow connections
            attachments = SlackNotification.construct_slack_message(
                context=context, status=status, pipeline=pipeline)
            slack_hook.post_webhook(attachments)
        elif not alert and status == 'failed':
            logger.warn(
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
