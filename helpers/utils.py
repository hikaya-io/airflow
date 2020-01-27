from pymongo import (MongoClient, UpdateOne,)
import psycopg2
from pandas.io.json._normalize import nested_to_record

from .configs import (
    POSTGRES_DB, POSTGRES_DB_PASSWORD, POSTGRES_USER,
    POSTGRES_HOST, POSTGRES_PORT
)

############################
#  Database Connections    #
############################
def establish_mongo_connection(mongo_uri, db_name):
    """
    establish MongoDB connection
    : param mongo_uri: mongo connection uri
    : return db_connection: database connection
    """
    client = MongoClient(mongo_uri)

    db_connection = client[db_name]
    return db_connection


def establish_postgres_connection():
    """
    establish postgres connection
    :return connection: database connection
    """
    connection = psycopg2.connect(
        database='{}'.format(POSTGRES_DB),
        user='{}'.format(POSTGRES_USER),
        password='{}'.format(POSTGRES_DB_PASSWORD),
        host='{}'.format(POSTGRES_HOST),
        port=POSTGRES_PORT)

    return connection


############################
#  Data Cleaning Utilities #
############################
def clean_column_names(data, columns, column_cleaning_params, database):
    """
    clean the column based on the user definitions from the variables
    remove unwanted columns and rename columns
    : param data:
    : param column_cleaning_params:
    : return data: data with cleaned column names
    """
    if database.lower() == 'mongo':
        for row in list(data):
            for key, value in list(row.items()):
                if key in columns:
                    # delete field if in columns
                    del row[key]
    else:
        for row in list(data):
            for key, value in list(row.items()):
                if key not in columns:
                    # delete field if not in columns
                    del row[key]
    # rename columns
    try:
        for row in list(data):
            for key in list(columns):
                new_key = ''
                if column_cleaning_params['XXXXX']:
                    new_key = key

                if column_cleaning_params['YYYYY']:
                    new_key = key

                if column_cleaning_params['ZZZZZ']:
                    new_key = key

                row[new_key] = row[key]
                del row[key]
        return data
    except (IndexError, KeyError):
        return data


def flatten_json_data(data, separator='_'):
    """
    flatten CommCare data (remove the json nesting)
    :param data: array of form submissions from
    :param separator: separator for the flattened Json
    :return clean:
    """
    flat_json_data = []

    for data_item in data:
        # use pandas 'nested_to_record' method to flatten json
        # separate levels with '_'
        flat_data_item = nested_to_record(data_item, sep=separator)

        flat_json_data.append(flat_data_item)

    return flat_json_data


def update_row_columns(fields, data):
    """
    Set missing column values
    :param fields: table fields
    :param data: data to be dumped
    :return data: updated data
    """
    columns = [item.get('name') for item in fields]
    for row_data in list(data):
        row_columns = row_data.keys()

        missing_columns = list(set(columns) - set(row_columns))

        if len(missing_columns) > 0:
            for column in missing_columns:
                field_obj = next(
                    filter(
                        lambda field: field.get('name') == column,
                        fields
                    )
                )
                row_data.setdefault(
                    column,
                    set_column_defaults(field_obj.get('type', None))
                )
    return data


def set_column_defaults(type):
    """
    Set data column default values for missing columns
    : param type: data type
    : return column_string: Postgres query compatible string
    """
    if type.lower() == 'int':
        return None

    if type.lower() == 'decimal':
        return None

    if type.lower() == 'char':
        return ''

    if type.lower() == 'boolean':
        return None

    else:
        return ''


############################
#  Postgres Operations     #
############################
def construct_postgres_create_table_query(table_name, columns_data):
    """
    construct postgres CREATE TABLE IF NOT EXISTS query
    :param table_name: name of the table to create
    :param columns_data: the data column names
    :return create_table_query: SQL query string
    """
    create_table_query = 'CREATE TABLE IF NOT EXISTS ' \
                         + table_name + ' (' + ', '.join(columns_data) + ')'

    return create_table_query


def construct_postgres_upsert_query(table_name, columns, target_column):
    """
    construct INSERT or UPDATE (UPSERT) query for postgres
    :param table_name: the table to upsert its row(s)
    :param columns: the table column names
    :param target_column: reference column for update
    :return full_upsert_query_string:  complete UPSERT query string
    """
    insert_query_string = 'INSERT INTO ' + table_name + '(' + ','\
        .join(columns) + ')'
    db_field_maps = ['%({})s'.format(item) for item in columns]
    # exclude_columns = ['EXCLUDE.{}'.format(column) for column in columns]
    # update_string = 'ON CONFLICT ({})'.format(target_column) +\
    #                 'DO UPDATE SET (' + ', '.join(columns) + ') = (' + \
    #                 ', '.join(exclude_columns) + ')'

    full_upsert_query_string = insert_query_string + 'VALUES (' + ','.join(
        db_field_maps) + ')'
    # + update_string

    return full_upsert_query_string


def construct_postgres_delete_query(table, column, values):
    """
    construct Postgres DELETE query for multiple records
    :param table: table to delete from
    :param column: the reference column for deletion
    :param values: list reference values to be deleted
    :return query: the DELETE query string
    """
    query = 'DELETE FROM {}'.format(table) + 'WHERE ' + \
            column + 'IN (' + ', '.joint(values) + ')'

    return query


def construct_column_strings(column_data):
    """
    Set data column names and the data_types
    : param column_data: column meta-data
    : return column_string: Postgres query compatible string
    """
    if column_data.get('type', None).lower() == 'int':
        return column_data.get('name') + ' INT'

    if column_data.get('type', None).lower() == 'decimal':
        return column_data.get('name') + ' REAL'

    if column_data.get('type', None).lower() == 'char':
        return column_data.get('name') + ' CHAR(' + str(column_data.get('length', 100)) + ')'

    if column_data.get('type', None).lower() == 'boolean':
        return column_data.get('name') + ' BOOLEAN'

    else:
        return column_data.get('name') + ' TEXT'


############################
#  Mongo Operations        #
############################
def construct_mongo_upsert_query(data, target_column):
    """
    construct mongo query to upsert documents
    :param data:
    :param target_column:
    :return:
    """

    # use bulk update MongoDB API to save the data
    unique_ids = [item.pop(target_column) for item in data]
    operations = [
        UpdateOne(
            {target_column: unique_id},
            {'$set': data},
            upsert=True
        ) for unique_id, data in zip(unique_ids, data)
    ]

    return operations

