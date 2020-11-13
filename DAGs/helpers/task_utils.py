import logging

from airflow.hooks.slack import SlackWebHook

from helpers.slack_utils import SlackNotification

logger = logging.getLogger(__name__)


def notify(status, pipeline, alert=True, connection='slack'):
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
