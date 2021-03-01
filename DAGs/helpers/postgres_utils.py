"""
Postgres Operations
"""
import psycopg2

from .configs import (
    POSTGRES_DB, POSTGRES_DB_PASSWORD, POSTGRES_USER,
    POSTGRES_HOST, POSTGRES_PORT,
)


class PostgresOperations:
    def __init__(self):
        pass

    @staticmethod
    def establish_postgres_connection(db_name):
        """
        establish postgres connection
        :return connection: database connection
        """
        connection = psycopg2.connect(
            database='{}'.format(db_name),
            user='{}'.format(POSTGRES_USER),
            password='{}'.format(POSTGRES_DB_PASSWORD),
            host='{}'.format(POSTGRES_HOST),
            port=POSTGRES_PORT
        )

        return connection

    @staticmethod
    def construct_postgres_create_table_query(table_name, columns_data):
        """
        construct postgres CREATE TABLE IF NOT EXISTS query
        :param table_name: name of the table to create
        :param columns_data: the data column names
        :return create_table_query: SQL query string
        """
        create_table_query = 'CREATE TABLE IF NOT EXISTS \"' \
                             + table_name + '\" (' + ', '.join(columns_data) + ')'
        return create_table_query

    @staticmethod
    def construct_postgres_upsert_query(table_name, columns, target_column):
        """
        construct INSERT or UPDATE (UPSERT) query for postgres
        :param table_name: the table to upsert its row(s)
        :param columns: the table column names
        :param target_column: reference column for update
        :return full_upsert_query_string:  complete UPSERT query string
        """
        insert_query_string = 'INSERT INTO \"' + table_name + '\" (\"' + '\", \"'.join(columns) + '\")'
        db_field_maps = ['%({})s'.format(item) for item in columns]
        exclude_columns = ['\"{}\"=excluded.\"{}\"'.format(column, column) for column in columns]
        update_string = 'ON CONFLICT (\"{}\") '.format(target_column) +\
                        'DO UPDATE SET ' + ', '.join(exclude_columns)

        full_upsert_query_string = insert_query_string + ' VALUES (' + ','.join(
            db_field_maps) + ') ' + update_string

        return full_upsert_query_string

    @staticmethod
    def construct_postgres_delete_query(table, column, values):
        """
        construct Postgres DELETE query for multiple records
        :param table: table to delete from
        :param column: the reference column for deletion
        :param values: list reference values to be deleted
        :return query: the DELETE query string
        """
        query = 'DELETE FROM {}'.format(table) + ' WHERE ' + \
                column + '= ANY(Array[' + ', '.join(["'{}'".format(str(item)) for item in values]) + '])'

        return query

    @staticmethod
    def construct_column_strings(column_data, primary_key):
        """
        Set data column names and the data_types
        : param column_data: column meta-data
        : return column_string: Postgres query compatible string
        """

        column_name = column_data.get('name', None)

        if column_name is None:
            column_name = column_data.get('db_name')

        if column_data.get('type', '').lower() == 'int':
            column_map = '\"' + column_name + '\" INT'

        elif column_data.get('type', '').lower() == 'decimal':
            column_map = '\"' + column_name + '\" REAL'

        elif column_data.get('type', '').lower() == 'char':
            column_map = '\"' + column_name + '\" CHAR(' + str(column_data.get('length', 100)) + ')'

        elif column_data.get('type', '').lower() == 'boolean':
            column_map = '\"' + column_name + '\" BOOLEAN'

        elif column_data.get('type', '').lower() == 'boolean':
            column_map = '\"' + column_name + '\" BOOLEAN'

        elif column_data.get('type', '').lower() == 'array':
            column_map = '\"' + column_name + '\" text[]'

        elif column_data.get('type', '').lower() == 'object' or \
                column_data.get('type', '').lower() == 'json':
            column_map = '\"' + column_name + '\" jsonb'

        else:
            column_map = '\"' + column_name + '\" TEXT'

        if column_name.lower() == str(primary_key).lower():
            column_map = '{} UNIQUE'.format(column_map)

        return column_map

    @staticmethod
    def get_all_row_ids_in_db(connection, primary_key, form):
        """
        Get all the list of primary_keys for data in postgres DB
        :param connection: Postgres DB connection
        :param primary_key: table's primary_key
        :return primary_key_id_list: list of the primary_key values
        """
        primary_key_values_list = []
        with connection:
            cursor = connection.cursor()
            cursor.execute("DECLARE super_cursor BINARY CURSOR FOR SELECT " + primary_key + " FROM " + form)

            while True:
                cursor.execute("FETCH 1000 FROM super_cursor")
                rows = cursor.fetchall()

                if not rows:
                    break

                row_ids = [str(item[0]).strip() for item in rows]

                primary_key_values_list = primary_key_values_list + row_ids

        return primary_key_values_list

