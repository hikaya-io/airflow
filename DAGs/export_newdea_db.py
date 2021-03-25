import os
import time
from urllib.parse import urljoin
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from requests import Session

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from helpers.task_utils import notify, get_daily_start_date
from helpers.utils import logger
from helpers.configs import (
    NEWDEA_BASE_URL, NEWDEA_USERNAME, NEWDEA_PASSWORD, FTP_SERVER_HOST, DAG_EMAIL, FTP_SERVER_USERNAME, 
    FTP_SERVER_PASSWORD, FTP_SERVER_EMAIL, FTP_SERVER_FOLDER, MSSQL_USERNAME, MSSQL_PASSWORD
)

DAG_NAME = 'newdea_LWF_data_export_pipeline'
PIPELINE = 'newdea'
sshHook = SSHHook(ssh_conn_id="ftp_msql_server")


default_args = {
    'owner': 'Hikaya',
    'depends_on_past': False,
    'start_date': get_daily_start_date(),
    'email': [DAG_EMAIL],
    'catchup': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'on_failure_callback': notify(status='failed', pipeline=PIPELINE),
    'on_success_callback': notify(status='success', pipeline=PIPELINE)
}

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


session = Session()
session.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                                'Chrome/88.0.4324.96 Safari/537.36'
session.headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,' \
                            '*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'


def login():
    logger.info('Logging in...')
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
        session.post(urljoin(NEWDEA_BASE_URL, 'Common/SignIn.aspx'),
                     data=data, allow_redirects=True)
        # This url has to be called to finalize login process
        res = session.get(urljoin(NEWDEA_BASE_URL, 'Portal/ListCenters.aspx?CHECK_PREFERRED_CENTER=true'),
                          allow_redirects=True)
        return res.ok

    return False


def check_for_active_export():
    url = urljoin(
        NEWDEA_BASE_URL, 'NonProfit/ProjectCenter/RapidSystemAdmin/DataExportLogReport.aspx')
    res = session.get(url)
    if res.ok:
        bs = BeautifulSoup(res.text, 'html.parser')
        status_columns = bs.select(
            'table[summary="DxLogTable"] tbody tr td:nth-child(7)')
        for col in status_columns:
            col_text = col.get_text().strip()
            if 'Queued' in col_text:
                return True

    return False


def get_export_status():
    url = urljoin(
        NEWDEA_BASE_URL, 'NonProfit/ProjectCenter/RapidSystemAdmin/DataExportLogReport.aspx')
    res = session.get(url)
    if res.ok:
        bs = BeautifulSoup(res.text, 'html.parser')
        status_columns = bs.select(
            'table[summary="DxLogTable"] tbody tr td:nth-child(7)')
        if len(status_columns):
            col_text = status_columns[0].get_text().strip()
            return 'Success' not in col_text, col_text

    return True, None


def do_export():
    url = urljoin(NEWDEA_BASE_URL,
                  'NonProfit/ProjectCenter/RapidSystemAdmin/DataExport.aspx')
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
            'ctl00$M$C$Content$ServerFolder$TB': FTP_SERVER_FOLDER,
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


restore_DB_command = """
set -e
cd /home/dots/newdea_backup

unset -v latest_export
for file in *.bak.zip; do
[[ $file -nt $latest_export ]] && latest_export=$file
done
shopt -s extglob
rm -fv !($latest_export)

unzip -t $latest_export
rm -fv ../sql_backup/*
unzip $latest_export -d ../sql_backup

export_file_name=`echo $latest_export | cut -d'.' -f 1`
backup_file=$export_file_name".bak"

cd ../sql_backup
cat > temp.sql <<- EOM
USE master;
GO
ALTER DATABASE newdea_db SET SINGLE_USER WITH ROLLBACK IMMEDIATE
RESTORE DATABASE [newdea_db] FROM  DISK = N'/var/opt/mssql/backup/exported_file.bak'
ALTER DATABASE newdea_db SET MULTI_USER;
GO
EOM
sed -i "s/exported_file.bak/$backup_file/g" temp.sql

echo -e "\n"$(date -u): "NEWDEA DB RESTORE STARTED (Using file: $backup_file)"
docker exec -i lwf_newdea /opt/mssql-tools/bin/sqlcmd -S localhost -U {} -P {} -i /var/opt/mssql/backup/temp.sql
echo $(date -u): "NEWDEA DB RESTORE ENDED"
""".format(MSSQL_USERNAME, MSSQL_PASSWORD)


def get_sql_query(file_name):
    absolute_path = os.path.dirname(os.path.abspath(__file__))
    file_path = absolute_path + "/helpers/" + file_name
    file = open(file_path, mode='r')
    query = file.read()
    file.close()
    return query


optimize_DB_command = """
set -e
cd /home/dots/sql_backup
cat > views.sql <<- EOM
{0}
docker exec -i lwf_newdea /opt/mssql-tools/bin/sqlcmd -S localhost -U {2} -P {3} -i /var/opt/mssql/backup/views.sql
cat > indices.sql <<- EOM
{1}
docker exec -i lwf_newdea /opt/mssql-tools/bin/sqlcmd -S localhost -U {2} -P {3} -i /var/opt/mssql/backup/indices.sql
""".format(get_sql_query("views.sql"), get_sql_query("indices.sql"), MSSQL_USERNAME, MSSQL_PASSWORD)

with DAG(DAG_NAME, default_args=default_args,
         schedule_interval='0 5 * * 1-5') as dag:
    export_db_from_newdea = PythonOperator(
        task_id='export_db_from_newdea',
        provide_context=True,
        python_callable=export_newdea_db,
        dag=dag
    )
    restore_newdea_db = SSHOperator(
        task_id='restore_newdea_db',
        command=restore_DB_command,
        ssh_hook=sshHook,
        dag=dag,
    )

    optimize_newdea_db = SSHOperator(
        task_id='optimize_newdea_db',
        command=optimize_DB_command,
        ssh_hook=sshHook,
        dag=dag,
    )

    export_db_from_newdea >> restore_newdea_db >> optimize_newdea_db
