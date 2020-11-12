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
    def set_footer_icon(pipeline):
        footer_icon = 'https://hikaya.io/assets/images/dots.png',
        footer_msg = 'Dots Data Pipeline Notification'
        if pipeline.lower() == 'ona':
            footer_icon = 'https://ona.io/img/onadata-logo.png'
            footer_msg = 'ONA Pipeline Notification'

        if pipeline.lower() == 'kobo':
            footer_icon = 'https://kobo.humanitarianresponse.info/static/img/kobologo.svg'
            footer_msg = 'KoboToolBox Pipeline Notification'

        if pipeline.lower() == 'commcare':
            footer_icon = 'https://blogs.unicef.org/wp-content/uploads/sites/2/2012/06/commcare.gif'
            footer_msg = 'CommCareHQ Pipeline Notification'

        if pipeline.lower() == 'surveycto':
            footer_icon = 'https://www.surveycto.com/wp-content/uploads/2018/04/SurveyCTO-Logo-CMYK.png'
            footer_msg = 'SurveyCTO Pipeline Notification'
        
        if pipeline.lower() == 'newdea':
            footer_icon = 'https://secureservercdn.net/50.62.172.232/35m.944.myftpupload.com/wp-content/uploads/2018/10/Logo_Horizontal_light.png'
            footer_msg = 'Newdea LWF Data Export Notification'
        
        if pipeline.lower() == 'mssql':
            footer_icon = 'https://cdn.worldvectorlogo.com/logos/microsoft-sql-server.svg'
            footer_msg = 'SQL Server Data Restore Notification'


        return dict(
            footer_icon=footer_icon,
            footer_msg=footer_msg
        )
    @classmethod
    def construct_slack_message(cls, context, status, pipeline):
        """
        construct slack notification message
        :param context: task context
        :param status: task status failed|success
        :param pipeline: the data pipeline ona|kobo|surveycto|commcare
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

        # set footer icon
        footer = cls.set_footer_icon(pipeline)
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
                'footer': footer.get('footer_msg', ''),
                'footer_icon': footer.get('footer_icon', '')
            }
        ]

        return  attachments