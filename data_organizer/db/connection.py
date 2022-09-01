import logging
from enum import Enum, auto
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import psycopg2
from pypika import Dialects, MySQLQuery, PostgreSQLQuery
from pypika.queries import Column, CreateQueryBuilder, QueryBuilder, Schema, Table
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError, OperationalError

from data_organizer.db.exceptions import (
    BinaryDataException,
    InvalidDataException,
    QueryReturnedNoData,
    TableNotExists,
)
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
        logger.debug(
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
        logger.debug("Closing connection")
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

    def query_inc_keys(
        self, query: Union[str, QueryBuilder]
    ) -> Tuple[List[Tuple[Any, ...]], List[str]]:
        """
        Execute the passed query.

        Args:
            query: Valid SQL queries as str or pypika.QueryBuilder

        Returns: List of results and list of column names
        """
        if isinstance(query, QueryBuilder):
            query = query.get_sql()

        logger.debug("Query: %s", query.replace("\n", " "))
        # TODO: Figure out why this still returns LegacyCurserResults
        with self.engine.connect() as connection:
            data = connection.execute(text(query))

        ret_data = [tuple(d) for d in list(data)]
        if not ret_data:
            raise QueryReturnedNoData

        return ret_data, data.keys()

    def query(self, query: Union[str, QueryBuilder]) -> List[Tuple[Any, ...]]:
        """
        Execute the passed query.

        Args:
            query: Valid SQL queries as str or pypika.QueryBuilder

        Returns: List of results
        """
        data, _ = self.query_inc_keys(query)
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

    def insert(
        self,
        table: TableSetting,
        datas: List[List[Any]],
        schema: Optional[str] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        Main insert method that includes validation and processing steps against the
        passed TableSettings objects.

        Args:
            table: TableSetting object defining the table data is inserted into
            datas: Data to be inserted

        Returns: Boolean flag denoting success of the insertion and Optional string
                 specifying the error
        """
        processed_data = self._preprocess_data_for_insert(table, datas)

        if table.disable_auto_insert_columns:
            inserted_columns = [c.name for c in table.columns]
        else:
            inserted_columns = [c.name for c in table.columns if c.is_inserted]

        return self._insert(table.name, inserted_columns, processed_data, schema=schema)

    def _preprocess_data_for_insert(
        self, table: TableSetting, datas: List[List[Any]]
    ) -> List[List[Any]]:
        """
        Preprocess the passed data for insertion

        Args:
            table: TableSetting object defining the table data is inserted into
            datas: Data to be processed

        Returns: Preprocessed data
        """
        processed_data = []
        for data in datas:
            this_processed_data = []
            if table.disable_auto_insert_columns:
                insert_columns = table.columns
            else:
                insert_columns = [c for c in table.columns if c.is_inserted]
            if len(data) != len(insert_columns):
                raise InvalidDataException(
                    "Number of passed data does not match number of columns expected"
                )
            for column, value in zip(insert_columns, data):
                if column.ctype == "BYTEA" and self.backend == Backend.POSTGRES:
                    if isinstance(value, str):
                        print(Path(value), Path(value).exists())
                        if Path(value).exists():
                            logger.debug(
                                "Passed value for BYTEA column is a valid path. "
                                "Assuming to read and insert content"
                            )
                            with open(value, "rb") as f:
                                value = f.read()
                    value = psycopg2.Binary(value)
                    try:
                        str(value)
                    except TypeError:
                        raise BinaryDataException(
                            "Value for column %s could not be converted to "
                            "binary properly" % column.name
                        )
                this_processed_data.append(value)
            processed_data.append(this_processed_data)

        return processed_data

    def _insert(
        self,
        table_name: str,
        columns: Optional[List[str]],
        data: List[List[Any]],
        schema: Optional[str] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        General purpose insert into database. Only using table_name and a lists
        of values.

        Args:
            table_name: Valid table name
            data: List containing Lists with valid values to insert
            schema: Optionally explicitly pass a schema if db is used w/o the
                    schema set in the init or used with one DatabaseConnection
                    instance over multiple schemas

        Returns: Boolean flag denoting success of the insertion and Optional string
                 specifying the error
        """
        if schema is None:
            table = Table(table_name)
        else:
            schema_ = Schema(schema)
            table = schema_.__getattr__(table_name)

        insert_statement = self.pypika_query.into(table)
        if columns is not None:
            insert_statement = insert_statement.columns(columns)
        for d in data:
            insert_statement = insert_statement.insert(*d)

        sql_insert_statement = insert_statement.get_sql()
        if len(sql_insert_statement) < 100:
            logger.debug(sql_insert_statement)

        data_inserted = True
        err_str = None
        with self.engine.connect() as connection:
            try:
                connection.execute(sql_insert_statement)
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
        self,
        creation_settings: List[TableSetting],
        foreign_key_settings: Dict[str, TableSetting] = {},
    ) -> None:
        """
        Creates a table based on the passed settings.

        Args:
            creation_settings: Nested dictionary containing the information to create
                               one or more tables
            foreign_key_settings:
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

            if table_info.name in foreign_key_settings:
                reference_table = foreign_key_settings[table_info.name]
                create_statement = create_statement.foreign_key(
                    columns=[Column(reference_table.rel_table_common_column)],
                    reference_table=Table(reference_table.name),
                    reference_columns=[Column(reference_table.rel_table_common_column)],
                )

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
