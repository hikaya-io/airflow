from airflow.models.variable import Variable

"""
Load all variables to be used in the Pipelines
"""

# Postgress Settings
POSTGRES_DB_PASSWORD = Variable.get('POSTGRES_DB_PASSWORD', default_var='')
POSTGRES_DB = Variable.get('POSTGRES_DB', default_var='')
POSTGRES_HOST = Variable.get('POSTGRES_HOST', default_var='')
POSTGRES_USER = Variable.get('POSTGRES_USER', default_var='')
POSTGRES_PORT = Variable.get('POSTGRES_PORT', default_var='')

# MongoDB Settings
MONGO_DB_USER = Variable.get('MONGO_DB_USER', default_var='')
MONGO_DB_PASSWORD = Variable.get('MONGO_DB_PASSWORD', default_var='')
MONGO_DB_HOST = Variable.get('MONGO_DB_HOST', default_var='127.0.0.1')
MONGO_DB_PORT = Variable.get('MONGO_DB_PORT', default_var=27017)

# SurveyCTO Variables #
SURV_SERVER_NAME = Variable.get('SURV_SERVER_NAME', default_var='')
SURV_FORMS = Variable.get('SURV_FORMS', deserialize_json=True)
SURV_PASSWORD = Variable.get('SURV_PASSWORD', default_var='127.0.0.1')
SURV_USERNAME = Variable.get('SURV_USERNAME', default_var=27017)

