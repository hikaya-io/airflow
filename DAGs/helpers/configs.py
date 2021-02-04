from airflow.models.variable import Variable

"""
Load all variables to be used in the Pipelines
"""

"""
Postgress Settings
"""
POSTGRES_DB_PASSWORD = Variable.get('POSTGRES_DB_PASSWORD', default_var='')
POSTGRES_DB = Variable.get('POSTGRES_DB', default_var='')
POSTGRES_HOST = Variable.get('POSTGRES_HOST', default_var='')
POSTGRES_USER = Variable.get('POSTGRES_USER', default_var='')
POSTGRES_PORT = Variable.get('POSTGRES_PORT', default_var='')

"""
MongoDB Settings
"""
MONGO_DB_USER = Variable.get('MONGO_DB_USER', default_var='')
MONGO_DB_PASSWORD = Variable.get('MONGO_DB_PASSWORD', default_var='')
MONGO_DB_HOST = Variable.get('MONGO_DB_HOST', default_var='127.0.0.1')
MONGO_DB_PORT = Variable.get('MONGO_DB_PORT', default_var=27017)


"""
Slack setting
"""
SLACK_CONN_ID = Variable.get('SLACK_CONN_ID', default_var='slack')

"""
DAG configurations
"""
DAG_OWNER = Variable.get('DAG_OWNER', default_var='Hikaya-Dots')
DAG_EMAIL = Variable.get('DAG_EMAIL', default_var='amos@hikaya.io')
DAG_EMAIL_ON_FAILURE = Variable.get('DAG_EMAIL_ON_FAILURE', default_var=True)
DAG_EMAIL_ON_RETRY = Variable.get('DAG_EMAIL_ON_RETRY', default_var=False)
DAG_NO_OF_RETRIES = Variable.get('DAG_NO_OF_RETRIES', default_var=2)
DAG_RETRY_DELAY_IN_MINS = Variable.get('DAG_RETRY_DELAY_IN_MINS', default_var=1)


"""
SurveyCTO Variables
"""
SURV_SERVER_NAME = Variable.get('SURV_SERVER_NAME', default_var='')
SURV_FORMS = Variable.get('SURV_FORMS', deserialize_json=True)
SURV_PASSWORD = Variable.get('SURV_PASSWORD', default_var='')
SURV_USERNAME = Variable.get('SURV_USERNAME', default_var='')
SURV_MONGO_URI = Variable.get('SURV_MONGO_URI', default_var='')
SURV_DBMS = Variable.get('SURV_DBMS', default_var=None)
SURV_MONGO_DB_NAME = Variable.get('SURV_MONGO_DB_NAME', default_var='')
SURV_RECREATE_DB = Variable.get('SURV_RECREATE_DB', default_var=False)

"""
ONA Variables
"""
ONA_API_URL = Variable.get('ONA_API_URL', default_var='')
ONA_TOKEN = Variable.get('ONA_TOKEN', default_var='')
ONA_MONGO_URI = Variable.get('ONA_MONGO_URI', default_var='')
ONA_MONGO_DB_NAME = Variable.get('ONA_MONGO_DB_NAME', default_var='')
ONA_DBMS = Variable.get('ONA_DBMS', default_var=None)
ONA_POSTGRES_DB_NAME = Variable.get('ONA_POSTGRES_DB_NAME', default_var=None)
ONA_RECREATE_DB = Variable.get('ONA_POSTGRES_DB_NAME', default_var='ona_data')
ONA_FORMS = Variable.get('ONA_FORMS', deserialize_json=True, default_var=None)

"""
Kobo Variables
"""
KOBO_API_URL = Variable.get('KOBO_API_URL', default_var='')
KOBO_TOKEN = Variable.get('KOBO_TOKEN', default_var=True)
KOBO_MONGO_URI = Variable.get('KOBO_MONGO_URI', default_var='')
KOBO_MONGO_DB_NAME = Variable.get('KOBO_MONGO_DB_NAME', default_var='')
KOBO_DBMS = Variable.get('KOBO_DBMS', default_var=None)
KOBO_RECREATE_DB = Variable.get('KOBO_RECREATE_DB', default_var=False)
KOBO_FORMS = Variable.get('KOBO_FORMS', deserialize_json=True, default_var=None)
KOBO_POSTGRES_DB_NAME = Variable.get('KOBO_POSTGRES_DB_NAME', default_var=None)

"""
Newdea Variables
"""
NEWDEA_BASE_URL = Variable.get(
    'NEWDEA_BASE_URL', default_var='https://logan.newdea.com/')
NEWDEA_USERNAME = Variable.get('NEWDEA_USERNAME', default_var='')
NEWDEA_PASSWORD = Variable.get('NEWDEA_PASSWORD', default_var='')
FTP_SERVER_HOST = Variable.get('FTP_SERVER_HOST', default_var='')
FTP_SERVER_USERNAME = Variable.get('FTP_SERVER_USERNAME', default_var='')
FTP_SERVER_PASSWORD = Variable.get('FTP_SERVER_PASSWORD', default_var='')
FTP_SERVER_EMAIL = Variable.get('FTP_SERVER_EMAIL', default_var='')

"""
SQL Server Variables
"""
MSSQL_USERNAME = Variable.get('MSSQL_USERNAME', default_var='')
MSSQL_PASSWORD = Variable.get('MSSQL_PASSWORD', default_var='')