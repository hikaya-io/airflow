#######################################################################
#         WORK IN PROGRESS                                            #
#   * Explore usage of SQLAlchemy Core, althoug saving submissions is #
#   now one using pandas.to_sql function.                             #
#   * Explore using SQLAlchemy core to separate DBs by server         #
#######################################################################

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
            echo=False,
        )

