from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks import SSHHook
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.utils.dates import days_ago

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta

import time

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

def export_newdea_db(**context):
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=chrome_options)
    driver.implicitly_wait(30)

    print('Launching Newdea')
    driver.get(NEWDEA_BASE_URL)

    # Login element locators
    txt_username = driver.find_element(
        By.XPATH, '//*[@id="M_C_SI_LC_UserName"]/*/input')
    txt_password = driver.find_element(
        By.XPATH, '//*[@id="M_C_SI_LC_Password"]/*/input')
    btn_signin = driver.find_element(
        By.XPATH, '//*[@id="M_C_SI_LC_LoginButton"]')

    # Login to Newdea
    print('Login to Newdea')
    txt_username.clear()
    txt_username.send_keys(NEWDEA_USERNAME)
    txt_password.clear()
    txt_password.send_keys(NEWDEA_PASSWORD)
    btn_signin.click()
    assert 'Newdea Project Center' in driver.title

    print('Verify no active export via Data Export Log Page')
    driver.get('{}{}'.format(
        NEWDEA_BASE_URL, 'NonProfit/ProjectCenter/RapidSystemAdmin/DataExportLogReport.aspx'))

    active_export = True
    wait_count = 1

    while active_export:
        export_log_title = driver.find_element(
            By.XPATH, '//*[@id="divMainPaneContent"]/*/h2')
        assert 'Data Export Log' in export_log_title.text
        export_status = driver.find_element(
            By.XPATH, '//*[@id="ctl00_M_C_Content_G_ctl00__0"]/td[7]/span')
        print('Status: ' + export_status.text)

        if export_status.text not in ['Queued', '']:
            # Display export page
            print('Display export page')
            driver.get('{}{}'.format(
                NEWDEA_BASE_URL, 'NonProfit/ProjectCenter/RapidSystemAdmin/DataExport.aspx'))
            export_title = driver.find_element(
                By.XPATH, '//*[@id="divMainPaneContent"]/*/h2')
            assert 'Export Data' in export_title.text
            print('Run data export via ftp')
            # Data Export form element locators
            txt_ftp_server = driver.find_element(
                By.XPATH, '//*[@id="ctl00_M_C_Content_ServerName_TB"]')
            txt_ftp_username = driver.find_element(
                By.XPATH, '//*[@id="ctl00_M_C_Content_UserName_TB"]')
            txt_ftp_password = driver.find_element(
                By.XPATH, '//*[@id="ctl00_M_C_Content_Password_TB"]')
            txt_ftp_email = driver.find_element(
                By.XPATH, '//*[@id="ctl00_M_C_Content_EmailAddress_TB"]')
            btn_submit_export_job = driver.find_element(
                By.XPATH, '//*[@id="ctl00_M_C_Content_Submit"]')

            txt_ftp_server.clear()
            txt_ftp_server.send_keys(FTP_SERVER_HOST)
            txt_ftp_username.clear()
            txt_ftp_username.send_keys(FTP_SERVER_USERNAME)
            txt_ftp_password.clear()
            txt_ftp_password.send_keys(FTP_SERVER_PASSWORD)
            txt_ftp_email.clear()
            txt_ftp_email.send_keys(FTP_SERVER_EMAIL)
            btn_submit_export_job.click()
            alert = driver.switch_to.alert
            alert.accept()

            print('Verify export')
            driver.get('{}{}'.format(
                NEWDEA_BASE_URL, 'NonProfit/ProjectCenter/RapidSystemAdmin/DataExportLogReport.aspx'))
            export_log_title = driver.find_element(
                By.XPATH, '//*[@id="divMainPaneContent"]/*/h2')
            assert 'Data Export Log' in export_log_title.text

            wait_count = 1

            while active_export:
                export_status = driver.find_element(
                    By.XPATH, '//*[@id="ctl00_M_C_Content_G_ctl00__0"]/td[7]/span')
                print('Status: ' + export_status.text)
                time.sleep(60)
                print('TOTAL CURRENT EXPORT WAIT TIME: {}min'.format(wait_count))

                if export_status.text == 'Complete (Error)':
                    driver.quit()
                    raise NewdeaError('Export Failed!')

                if wait_count == 60:
                    driver.quit()
                    raise NewdeaError('Export takes too long to complete!')

                if export_status.text != 'Complete (Success)':
                    wait_count += 1
                    driver.refresh()
                else:
                    active_export = False
                    driver.quit()

        else:
            time.sleep(180)
            print('TOTAL PREVIOUS EXPORT WAIT TIME: {}min'.format(30*wait_count))
            if wait_count == 3:
                driver.quit()
            wait_count += 1
            driver.refresh()

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
