import logging
from enum import Enum, auto
from typing import Any, List, Optional, Tuple

import pandas as pd
from pypika import Dialects, MySQLQuery, PostgreSQLQuery
from pypika.queries import Column, CreateQueryBuilder, Table
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError, OperationalError

from data_organizer.db.exceptions import QueryReturnedNoData, TableNotExists
from data_organizer.db.model import ColumnSetting, TableSetting

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
        name: str = "DataOrganizer",
    ):
        url = f"{prefix}://{user}:{password}@{host}:{port}/{database}"
        logger.info(
            "Engine URL: %s",
            url.replace(":" + password + "@", ":" + len(password) * "?" + "@"),
        )

        if "mysql" in prefix:
            self.backend = Backend.MYSQL
            self.dialect = Dialects.MYSQL
            self.pypika_query = MySQLQuery
        elif "postgresql" in prefix:
            self.backend = Backend.POSTGRES
            self.dialect = Dialects.POSTGRESQL
            self.pypika_query = PostgreSQLQuery
        else:
            raise NotImplementedError(
                "Currently only %s are supported as backends"
                % (",".join(Backend.avail_backends()))
            )

        connect_args = {}
        if self.backend == Backend.POSTGRES:
            connect_args.update({"application_name": name})
            if schema is not None:
                connect_args.update({"options": f"-csearch_path={schema},public"})

        self.engine = create_engine(
            url,
            echo=verbose,
            connect_args=connect_args,
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
        if schema is not None and self.is_valid:
            with self.engine.connect() as connection:
                avail_schemas = connection.execute(
                    "SELECT schema_name FROM information_schema.schemata"
                )
                if schema not in [x[0] for x in avail_schemas]:
                    logger.info("Will  create schema: %s", schema)
                    connection.execute(f"CREATE SCHEMA {schema}")

        self.created_tables: List[str] = []

    def close(self) -> None:
        """Close the connection"""
        logger.info("Closing connection")
        self.engine.dispose()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def query_to_df(self, sql: str) -> pd.DataFrame:
        """
        Function wrapping a SQL query using the engine

        Args:
          sql : Valid SQL query
        """
        logger.debug("Query: %s", sql.replace("\n", " "))
        with self.engine.connect() as connection:
            data: pd.DataFrame = pd.read_sql_query(sql, connection)

        if data.empty:
            raise QueryReturnedNoData

        return data

    def insert_df(
        self, table_name: str, data: pd.DataFrame, if_exists: str = "append"
    ) -> Tuple[bool, Optional[str]]:
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
        err_str = None
        try:
            with self.engine.connect() as connection:
                data.to_sql(
                    table_name, con=connection, if_exists=if_exists, index=False
                )
                data_inserted = True
        except IntegrityError as e:
            logger.error("Data could not be inserted: %s", str(e))
            data_inserted = False
            err_str = str(e)

        return data_inserted, err_str

    def _insert(
        self, table_name: str, data: List[List[Any]]
    ) -> Tuple[bool, Optional[str]]:
        """
        General purpose insert into database. Only using table_name and a lists
        of values.

        Args:
            table_name: Valid table name
            data: List containing Lists with valid values to insert

        Returns: Boolean flag denoting success of the insertion and Optional string
                 specifying the error
        """
        table = Table(table_name)

        insert_statement = self.pypika_query.into(table)
        for d in data:
            insert_statement = insert_statement.insert(*d)

        logger.debug(insert_statement.get_sql())

        data_inserted = True
        err_str = None
        with self.engine.connect() as connection:
            try:
                connection.execute(insert_statement.get_sql())
            except IntegrityError as e:
                logger.error("Data could not be inserted: %s", str(e))
                data_inserted = False
                err_str = str(e)

        return data_inserted, err_str

    def has_table(self, table_name: str) -> bool:
        """
        Check if the passed table exits in the active connection

        Args:
            table_name: Name of the table

        Returns:
            True if table exists, False otherwise
        """
        with self.engine.connect() as connection:
            has_table = inspect(connection).has_table(table_name)
        if has_table:
            return True
        else:
            return False

    def create_table_from_table_info(
        self, creation_settings: List[TableSetting]
    ) -> None:
        """
        Creates a table based on the passed settings.

        Args:
            creation_settings: Nested dictionary containing the information to create
                               one or more tables
        """
        for table_info in creation_settings:
            create_columns = []
            unique_columns = []
            primary_columns = []
            for column_info in table_info.columns:
                create_columns.append(
                    Column(
                        column_name=column_info.name,
                        column_type=column_info.ctype,
                        nullable=column_info.is_nullable,
                    )
                )
                if column_info.is_unique:
                    unique_columns.append(column_info.name)
                if column_info.is_primary:
                    primary_columns.append(column_info.name)
            create_statement = (
                CreateQueryBuilder(dialect=self.dialect)
                .create_table(table_info.name)
                .columns(*create_columns)
            )
            if unique_columns:
                create_statement = create_statement.unique(*unique_columns)
            if primary_columns:
                create_statement = create_statement.primary_key(*primary_columns)

            logger.info("Creating table %s", table_info.name)
            logger.debug(create_statement.get_sql())

            with self.engine.connect() as connection:
                connection.execute(create_statement.get_sql())

    def add_column_to_table(self, table_name: str, new_column: ColumnSetting) -> None:
        """
        Add a new column to an existing table

        Args:
            table_name: Name of the table
            new_column: Settings for the now column
        """
        raise NotImplementedError
