import logging
from datetime import datetime, timedelta, time

from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from helpers.slack_utils import SlackNotification


def notify(context, status, pipeline):
    """
    callback function used to alert of task failures and successes
    """

    alert = SlackWebhookOperator(
        task_id='slack_alert',
        http_conn_id='slack',
        attachments=SlackNotification.construct_slack_message(
            context,
            status,
            pipeline
        )
    )

    return alert.execute(context=context)


def get_daily_start_date(days=-1, hour=0, min=0):
    """
    set start_date to most recent occurrence
    """

    dt = datetime.utcnow()
    desired_start_date = dt + timedelta(days=days)

    return datetime.combine(desired_start_date, time(hour, min))
