"""
Slack notifications
"""
class SlackNotification:
    """
    slack notification class
    """
    def __init__(self):
        pass

    @staticmethod
    def construct_slack_message(context, status):
        """
        construct slack notification message
        :param context: task context
        :param status: task status failed | success
        :return attachments: slack message attachments
        """
        if status == 'success':
            notification_message = 'Successful!!! :tada: :tada: :rocket: :rocket:'
            message_imoji = ':white_check_mark:'

        else:
            notification_message = 'Failed!!! :octagonal_sign: :octagonal_sign: :disappointed: :disappointed:'
            message_imoji = ':red_circle:'
        slack_msg = """
        {message_imoji} Task Successful. 
        *Task*: {task}  
        *Dag*: {dag} 
        *Execution Time*: {exec_date}
        """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            message_imoji=message_imoji
        )

        task_name = context.get('task_instance').task_id.replace('_', ' ').title()
        attachments = [
            {
                'fallback': slack_msg,
                'color': '#E52C2C' if status == 'failed' else '#25CED1',
                'pretext': f'Dots data pipeline: *{task_name}*',
                'author_link': 'https://hikaya.io',
                'author_icon': 'https://hikaya.io/assets/images/dots.png',
                'title': 'Hikaya Data Pipeline Alert',
                'title_link': context.get('task_instance').log_url,
                'text': f':clock1: *{str(context.get("execution_date").ctime())}*',
                'fields': [
                    {
                        'title': 'DAG',
                        'value': context.get('task_instance').dag_id,
                        'short': False
                    },
                    {
                        'title': 'Task',
                        'value': context.get('task_instance').task_id,
                        'short': False
                    },
                    {
                        'title': 'Status',
                        'value':notification_message,
                        'short': False
                    }
                ],
                'footer': 'Hikaya Airflow',
                'footer_icon': 'https://hikaya.io/assets/images/dots.png'
            }
        ]

        return  attachments