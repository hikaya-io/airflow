from urllib.parse import urljoin

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

from datetime import datetime, timedelta

import time

from bs4 import BeautifulSoup
from requests import Session

from DAGs.helpers.utils import logger
from helpers.slack_utils import (SlackNotification, )
from helpers.configs import (
    NEWDEA_BASE_URL, NEWDEA_USERNAME, NEWDEA_PASSWORD, FTP_SERVER_HOST, DAG_EMAIL,
    FTP_SERVER_USERNAME, FTP_SERVER_PASSWORD, FTP_SERVER_EMAIL, SLACK_CONN_ID,
    MSSQL_USERNAME, MSSQL_PASSWORD
)

"""
Custom expception for Newdea issues
"""


class NewdeaError(Exception):
    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self):
        if self.message:
            return '{0}.'.format(self.message)
        else:
            return 'NewdeaError has been raised'


default_args = {
    'owner': 'Hikaya',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': [DAG_EMAIL],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG(
    'newdea_LWF_data_export_pipeline',
    default_args=default_args,
    schedule_interval='0 0 * * 0,3',
)

sshHook = SSHHook(ssh_conn_id="ftp_msql_server")
slack_notification = SlackNotification()

session = Session()
session.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                                'Chrome/88.0.4324.96 Safari/537.36'
session.headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,' \
                            '*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'


def login():
    print('Logging in...')
    res = session.get(NEWDEA_BASE_URL)
    if res.ok:
        bs = BeautifulSoup(res.text, 'html.parser')
        data = {
            '__EVENTTARGET': bs.select_one('#__EVENTTARGET').attrs['value'],
            '__EVENTARGUMENT': bs.select_one('#__EVENTARGUMENT').attrs['value'],
            'DES_Group': bs.select_one('#DES_Group').attrs['value'],
            '__VIEWSTATE1': bs.select_one('#__VIEWSTATE1').attrs['value'],
            '__VIEWSTATE': bs.select_one('#__VIEWSTATE').attrs['value'],
            'M$C$SI$LC$UserName$TB': NEWDEA_USERNAME,
            'M$C$SI$LC$Password$TB': NEWDEA_PASSWORD,
            'M$C$SI$LC$LoginButton': bs.select_one('#M_C_SI_LC_LoginButton').attrs['value'],
        }
        session.post(urljoin(NEWDEA_BASE_URL, 'Common/SignIn.aspx'), data=data, allow_redirects=True)
        # This url has to be called to finalize login process
        res = session.get(urljoin(NEWDEA_BASE_URL, 'Portal/ListCenters.aspx?CHECK_PREFERRED_CENTER=true'),
                          allow_redirects=True)
        return res.ok

    return False


def check_for_active_export():
    url = urljoin(NEWDEA_BASE_URL, 'NonProfit/ProjectCenter/RapidSystemAdmin/DataExportLogReport.aspx')
    res = session.get(url)
    if res.ok:
        bs = BeautifulSoup(res.text, 'html.parser')
        status_columns = bs.select('table[summary="DxLogTable"] tbody tr td:nth-child(7)')
        for col in status_columns:
            col_text = col.get_text().strip()
            if 'Queued' in col_text:
                return True

    return False


def get_export_status():
    url = urljoin(NEWDEA_BASE_URL, 'NonProfit/ProjectCenter/RapidSystemAdmin/DataExportLogReport.aspx')
    res = session.get(url)
    if res.ok:
        bs = BeautifulSoup(res.text, 'html.parser')
        status_columns = bs.select('table[summary="DxLogTable"] tbody tr td:nth-child(7)')
        if len(status_columns):
            col_text = status_columns[0].get_text().strip()
            return 'Success' not in col_text, col_text

    return True, None


def do_export():
    url = urljoin(NEWDEA_BASE_URL, 'NonProfit/ProjectCenter/RapidSystemAdmin/DataExport.aspx')
    res = session.get(url)
    if res.ok:
        bs = BeautifulSoup(res.text, 'html.parser')
        data = {
            '__EVENTTARGET': bs.select_one('#__EVENTTARGET').attrs['value'],
            '__EVENTARGUMENT': bs.select_one('#__EVENTARGUMENT').attrs['value'],
            'DES_Group': bs.select_one('#DES_Group').attrs['value'],
            'DES_ChgMon': bs.select_one('#DES_ChgMon').attrs['value'],
            'DES_LinkIDState': bs.select_one('#DES_LinkIDState').attrs['value'],
            'DES_ScriptFileIDState': bs.select_one('#DES_ScriptFileIDState').attrs['value'],
            '__VIEWSTATE1': bs.select_one('#__VIEWSTATE1').attrs['value'],
            '__VIEWSTATE': bs.select_one('#__VIEWSTATE').attrs['value'],
            'ctl00$M$C$Content$ServerName$TB': FTP_SERVER_HOST,
            'ctl00$M$C$Content$ServerFolder$TB': '',
            'ctl00$M$C$Content$UserName$TB': FTP_SERVER_USERNAME,
            'ctl00$M$C$Content$Password$TB': FTP_SERVER_PASSWORD,
            'ctl00$M$C$Content$EmailAddress$TB': FTP_SERVER_EMAIL,
            'ctl00$M$C$Content$ExportAttachments$DDL': 'None',
            'ctl00$M$C$Content$UseCompression$CB': 'on',
            'ctl00$M$C$Content$BackupPassword$TB': '',
            'ctl00$M$C$Content$ConfirmBackupPassword$TB': '',
            'ctl00$M$C$Content$Submit': 'Submit+Export+Job',
        }
        res = session.post(url, data=data, allow_redirects=True)
        if res.ok:
            return 'The export job has been queued.' in res.text

    return False


def export_newdea_db(**context):
    if login():
        active_export = check_for_active_export()
        no_of_checks = 1
        while active_export:
            if no_of_checks == 3:
                logger.info('Done 3 checks already. Terminating..')
                return

            logger.info('Export ongoing, sleeping for 3 minutes...')
            time.sleep(180)
            active_export = check_for_active_export()
            no_of_checks += 1

        logger.info('No ongoing export found. Starting new export process...')
        active_export = do_export()
        no_of_checks = 1
        while active_export:
            logger.info(f'Total wait time: {no_of_checks - 1} min(s)')
            if no_of_checks == 60:
                logger.info('Done 60 checks already. Terminating..')
                raise NewdeaError('Export taking too long to complete!')

            active_export, status = get_export_status()

            if active_export:
                if status == 'Complete (Error)':
                    raise NewdeaError('Export Failed!')

                no_of_checks += 1

            time.sleep(60)

        logger.info('Export done!')
    else:
        raise NewdeaError('Unable to login!')


def task_success_slack_notification(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    att_pipeline = 'mssql' if context['task_instance'].task_id == 'restore_newdea_db' else 'newdea'
    attachments = slack_notification.construct_slack_message(
        context,
        'success',
        att_pipeline
    )

    success_alert = SlackWebhookOperator(
        task_id='slack_alert_success',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        attachments=attachments,
        username='airflow'
    )
    return success_alert.execute(context=context)


def task_failed_slack_notification(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    att_pipeline = 'mssql' if context['task_instance'].task_id == 'restore_newdea_db' else 'newdea'
    attachments = slack_notification.construct_slack_message(
        context,
        'failed',
        att_pipeline
    )

    failed_alert = SlackWebhookOperator(
        task_id='slack_alert_failed',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        attachments=attachments,
        username='airflow')
    return failed_alert.execute(context=context)


restore_DB_command = """
set -e
cd /home/dots/
unset -v latest_export
rm -fv *.bak
for file in *.bak.zip; do
  [[ $file -nt $latest_export ]] && latest_export=$file
done

export_file=`echo $latest_export | cut -d'.' -f 1`

if unzip -t $export_file".bak.zip"
then
echo -e "\n"$(date -u): "NEWDEA DB RESTORE STARTED (Using file: $latest_export)"
backupfile=$export_file".bak"
unzip $export_file".bak.zip"

cat > temp.sql <<- EOM
USE master;
GO
ALTER DATABASE newdea_db SET SINGLE_USER WITH ROLLBACK IMMEDIATE
RESTORE DATABASE [newdea_db] FROM  DISK = N'/home/dots/exported_file.bak'
ALTER DATABASE newdea_db SET MULTI_USER;
GO
EOM

sed -i "s/exported_file.bak/$backupfile/g" temp.sql
sqlcmd -S localhost -U {} -P {} -i temp.sql
rm -f temp.sql $backupfile export_backup/*
mv $export_file".bak.zip" export_backup/

echo $(date -u): "NEWDEA DB RESTORE ENDED"

fi
""".format(MSSQL_USERNAME, MSSQL_PASSWORD)

run_data_export_from_newdea = PythonOperator(
    task_id='export_db_from_newdea',
    provide_context=True,
    python_callable=export_newdea_db,
    on_failure_callback=task_failed_slack_notification,
    on_success_callback=task_success_slack_notification,
    dag=dag
)

restore_newdea_db = SSHOperator(
    task_id='restore_newdea_db',
    command=restore_DB_command,
    ssh_hook=sshHook,
    on_failure_callback=task_failed_slack_notification,
    on_success_callback=task_success_slack_notification,
    dag=dag,
)

run_data_export_from_newdea >> restore_newdea_db

if __name__ == '__main__':
    export_newdea_db()
