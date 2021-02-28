"""
General utility functions
Data Cleaning Utilities
"""
from pandas.io.json._normalize import nested_to_record
import json
import logging

# Imported in other files
logger = logging.getLogger("airflow.task")

class DataCleaningUtil:
    def __init__(self):
        pass

    @staticmethod
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

    @staticmethod
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

    @staticmethod
    def set_column_defaults(type):
        """
        Set data column default values for missing columns
        : param type: data type
        : return column_string: Postgres query compatible string
        """

        return None

        if type.lower() == 'decimal':
            return None

        if type.lower() == 'char':
            return ''

        if type.lower() == 'boolean':
            return None

        if type.lower() == 'array':
            return None

        if type.lower() == 'object' or type.lower() == 'json':
            return None

        else:
            return ''

    @classmethod
    def update_row_columns(cls, fields, data):
        """
        Set missing column values
        :param fields: table fields
        :param data: data to be dumped
        :return data: updated data
        """
        columns = [item.get('name') or item.get('db_name') for item in fields]
        for row_data in list(data):
            if row_data is not None:
                row_columns = list(row_data.keys())
                
            else:
                row_columns = []

            missing_columns = list(set(columns) - set(row_columns))

            if len(missing_columns) > 0:
                for column in missing_columns:
                    field_obj = next(
                        filter(
                            lambda field: field.get('name') or item.get('db_name') == column,
                            fields
                        )
                    )
                    if row_data is not None:
                        row_data.setdefault(
                            column,
                            cls.set_column_defaults(field_obj.get('type', None))
                        )
        return data

    @staticmethod
    def clean_key_field(row, primary_key):
        """
        strip off string 'uuid:' from key field
        :param row: the row object
        :param primary_key: primary key
        :return row: row object with cleaned key field
        """
        row[primary_key] = row.get(primary_key).replace('uuid:', '').strip()

        return row

    @staticmethod
    def json_stringify_colum_data(row, fields=None):
        """
        stringify json object to prevent postgress from complaining
        :param row:
        :return:
        """
        for key, value in row.items():
            if isinstance(value, (list, dict,)):
                column_value = row.pop(key)
                if len(list(value)) > 0:
                    row.setdefault(key, '{ARRAY' + json.dumps(column_value) + '}')
                else:
                    row.setdefault(key, None)

            if value is not None and isinstance(value, (str,)) and 'uuid:' in str(value):
                column_value = row.pop(key)
                row.setdefault(key, column_value.replace('uuid:', '').strip())
        return row
