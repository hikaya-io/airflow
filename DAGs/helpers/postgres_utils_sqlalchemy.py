import sqlalchemy
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    Integer,
    String,
    MetaData,
    ForeignKey,
)


class PostgresOperations:
    def __init__(self, user, password, host, port, database):
        self.engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}",
            echo=True,
        )

    def create_database(name):
        print("create database")

    def create_table(self, name, columns):
        """Create a Postgres table

        Args:
            name (string): Name of the table
            columns (array): array of dicts, each defining `name` of the field and its `type`
                Allowed field types are the following: `string`, `integer`, `float`, `foreign_key?!` # TODO
                # TODO Primary keys, nullability, foreign keys
        """
        try:
            metadata = MetaData()
            columns = []
            table = Table(name, metadata, *columns)
            metadata.create_all(self.engine)

            return table
        except e:
            print('Exception')
            print(e)

    def upsert(table, value, on_conflict):
        """Upsert a value into a table

        Args:
            table ([type]): [description]
            value ([type]): [description]
            on_conflict ([type]): key(s) on which to detect conflicting upserts
        """
        print("upsert")
