import logging
from enum import Enum, auto
from typing import List

import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Connection
from sqlalchemy.exc import OperationalError

from data_organizer.db.exceptions import QueryReturnedNoData, TableNotExists

logger = logging.getLogger(__name__)


class Backend(Enum):
    MYSQL = auto()
    POSTGRES = auto()

    @staticmethod
    def avail_backends():
        return Backend._member_names_


class DatabaseConnection:
    """Wrapper for the database connection"""

    def __init__(
        self,
        user: str,
        password: str,
        database: str,
        host: str = "localhost",
        port: int = 5432,
        prefix: str = "postgresql+psycopg2",
        verbose: bool = False,
        schema: str = None,
    ):
        url = f"{prefix}://{user}:{password}@{host}:{port}/{database}"
        logger.info("Engine URL: %s", url)

        if "mysql" in prefix:
            self.backend = Backend.MYSQL
        elif "postgresql" in prefix:
            self.backend = Backend.POSTGRES
        else:
            raise NotImplementedError(
                "Currently only %s are supported as backends"
                % (",".join(Backend.avail_backends()))
            )

        self.engine = create_engine(
            url,
            echo=verbose,
            connect_args={"options": f"-csearch_path={schema},public"}
            if (self.backend == Backend.POSTGRES and schema is not None)
            else {},
        )
        connection: Connection
        try:
            logger.debug("Opening test connection")
            connection = self.engine.connect()
            self.is_valid = True
        except OperationalError as e:
            logger.error("%s", str(e))
            self.is_valid = False
        else:
            logger.debug("Closing test connection")
            connection.close()

        self.schema = schema
        if schema is not None:
            avail_schemas = self.engine.execute(
                "SELECT schema_name FROM information_schema.schemata"
            )
            if schema not in [x[0] for x in avail_schemas]:
                logger.info("Will  create schema: %s", schema)
                self.engine.execute(f"CREATE SCHEMA {schema}")

        self.created_tables: List[str] = []

    def close(self) -> None:
        """Close the connection"""
        logger.info("Closing connection")
        self.engine.dispose()

    def query(self, sql: str) -> pd.DataFrame:
        """
        Function wrapping a SQL query using the engine

        Args:
          sql : Valid SQL query
        """
        logger.debug("Query: %s", sql.replace("\n", " "))
        data: pd.DataFrame = pd.read_sql_query(sql, self.engine)

        if data.empty:
            raise QueryReturnedNoData

        return data

    def insert(
        self, table_name: str, data: pd.DataFrame, if_exists: str = "append"
    ) -> None:
        """
        Function to insert a DataFrame into the sql database

        Args:
            table_name: Name of the table to insert
            data: Data to insert
            if_exists: Setting for the if_exists argument in the pd.DataFrame.to_sql
        """
        if not self.has_table(table_name):
            raise TableNotExists("Table %s does not exists" % table_name)

        logger.debug(
            "Inserting data into table %s - if_exists = %s", table_name, if_exists
        )
        data.to_sql(table_name, con=self.engine, if_exists=if_exists, index=False)

    def has_table(self, table_name: str) -> bool:
        """
        Check if the passed table exits in the active connection

        Args:
            table_name: Name of the table

        Returns:
            True if table exists, False otherwise
        """
        if inspect(self.engine).has_table(table_name):
            return True
        else:
            return False
