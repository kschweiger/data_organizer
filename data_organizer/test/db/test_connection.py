import os
import uuid

import pandas as pd
import psycopg2
import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from data_organizer.db.connection import Backend, DatabaseConnection
from data_organizer.db.exceptions import (
    BinaryDataException,
    InvalidDataException,
    QueryReturnedNoData,
    TableNotExists,
)
from data_organizer.db.model import ColumnSetting, TableSetting
from data_organizer.utils import init_logging

init_logging("DEBUG")

if os.getenv("PG_DEV_DB_USER") is None or os.getenv("PG_DEV_DB_PASSWORD") is None:
    raise RuntimeError("Set $PG_DEV_DB_USER and $PG_DEV_DB_PASSWORD")

USER = os.getenv("PG_DEV_DB_USER")
PW = os.getenv("PG_DEV_DB_PASSWORD")
DBNAME = "Development"
SERVER = "localhost"


@pytest.fixture(scope="module")
def db():
    """Database connection for the tests"""
    the_connection = DatabaseConnection(USER, PW, DBNAME)
    yield the_connection
    the_connection.close()


@pytest.fixture(scope="module")
def engine():
    """Use another engine to create a test table"""
    url = f"postgresql+psycopg2://{USER}:{PW}@{SERVER}/{DBNAME}"
    test_engine = create_engine(url, echo=False)
    yield test_engine
    test_engine.dispose()


@pytest.fixture(scope="function")  # noqa: PT003
def test_table_create_drop(engine):
    """
    Create a testing table on which operation will be tested. This one should reset
    per test
    """
    test_uuid = str(uuid.uuid4()).replace("-", "_")
    table = f"test_table_{test_uuid}"
    with engine.connect() as connection:
        connection.execute(
            f"""
            CREATE TABLE {table} (
            id varchar(255) NOT NULL,
            col1 float DEFAULT NULL,
            col2 float DEFAULT NULL,
            PRIMARY KEY (id)
            )
            """
        )
        connection.execute(
            f"""
            INSERT INTO {table}
            VALUES ('A', 1.0, 2.0), ('B', 2.0, 3.0),('C', 1.0, 6.0),('D', 32.0, 2.0)
            """
        )
    # ---------------------------------------------------------------------------------
    yield table
    # ---------------------------------------------------------------------------------
    with engine.connect() as connection:
        connection.execute(
            f"""
            DROP TABLE {table}
            """
        )


def test_DatabaseConnection_init():
    db = DatabaseConnection(USER, PW, DBNAME)

    assert db.is_valid
    assert isinstance(db.engine, Engine)

    assert isinstance(db.backend, Backend)

    db.close()


def test_DatabaseConnection_init_invalid():
    db = DatabaseConnection(USER, PW, "BOGUSNAME")

    assert not db.is_valid


def test_query(db, test_table_create_drop):
    this_data = db.query_to_df(
        f"""
        SELECT *
        FROM {test_table_create_drop} t
        """
    )

    assert isinstance(this_data, pd.DataFrame)
    assert not this_data.empty

    with pytest.raises(QueryReturnedNoData):
        db.query_to_df(
            f"""
            SELECT *
            FROM {test_table_create_drop} t
            WHERE t.id = 'XYZ'
            """
        )


def test_insert_append(db, test_table_create_drop):
    data_to_insert = pd.DataFrame({"id": ["E"], "col1": [222.0], "col2": [222.0]})

    db.insert_df(
        table_name=test_table_create_drop, data=data_to_insert, if_exists="append"
    )

    inserted_data = db.query_to_df(
        f"""
        SELECT *
        FROM {test_table_create_drop} t
        WHERE t.id = 'E'
        """
    )
    assert data_to_insert.equals(inserted_data)

    all_data = db.query_to_df(
        f"""
        SELECT *
        FROM {test_table_create_drop} t
        """
    )

    assert not data_to_insert.equals(all_data)

    with pytest.raises(TableNotExists):
        db.insert_df(table_name="bogus_table", data=data_to_insert, if_exists="append")


def test_create_from_table_info(db):
    cols = {
        "A": {"ctype": "INT", "is_primary": True, "is_unique": True},
        "B": {"ctype": "INT", "is_primary": True},
        "C": {"ctype": "INT", "is_nullable": True},
        "D": {"ctype": "INT"},
    }
    creation_settings = [
        TableSetting(
            name="table_from_info_" + str(uuid.uuid4()).replace("-", "_"),
            columns=[ColumnSetting(name=name, **info) for name, info in cols.items()],
        )
    ]

    db.create_table_from_table_info(creation_settings)

    for table in creation_settings:
        assert db.has_table(table.name)
        db.engine.execute(f"DROP TABLE {table.name}")


def test_DatabaseConnection_schema():
    """Test if schemas are created correctly and tables are created inside it"""
    db = DatabaseConnection(USER, PW, DBNAME, schema="test_schema")

    res = db.engine.execute(
        """
        SELECT schema_name FROM information_schema.schemata;
        """
    ).all()

    res = [s[0] for s in res]

    assert db.schema in res

    test_table_name = "test_table_" + str(uuid.uuid4()).replace("-", "_")

    db.engine.execute(
        f"CREATE TABLE {test_table_name} (id INT NOT NULL, col FLOAT NOT NULL)"
    )

    tables = db.query_to_df(
        """
        SELECT table_name, MAX(table_schema) AS schema_name
        FROM information_schema.columns  AS tables
        GROUP BY tables.table_name;
        """
    ).set_index("table_name")

    assert tables.loc[[test_table_name]]["schema_name"].values[0] == db.schema

    db.engine.execute(f"DROP TABLE {db.schema}.{test_table_name}")
    db.engine.execute(f"DROP SCHEMA {db.schema}")

    db.close()


@pytest.mark.parametrize(
    ("data", "exp_ids"),
    [
        ([["E", 222.2, 111.1]], ["A", "B", "C", "D", "E"]),
        ([["X", 888.8, 999.9], ["Y", 777.7, 666.6]], ["A", "B", "C", "D", "X", "Y"]),
    ],
)
def test_underscore_insert(db, test_table_create_drop, data, exp_ids):
    ids_pre_insert = db.query_to_df(
        f"""
            SELECT t.id
            FROM {test_table_create_drop} t
            """
    )

    success, _ = db._insert(test_table_create_drop, data)
    assert success

    ids_post_insert = db.query_to_df(
        f"""
        SELECT t.id
        FROM {test_table_create_drop} t
        """
    )

    assert not ids_pre_insert.equals(ids_post_insert)

    assert ids_post_insert.id.to_list() == exp_ids


def test_preprocess_data_for_insert_w_serial(db):
    cols = {
        "A": {"ctype": "SERIAL", "is_primary": True, "is_inserted": False},
        "B": {"ctype": "INT"},
        "C": {"ctype": "INT"},
    }
    table_settings = TableSetting(
        name="table_from_info_" + str(uuid.uuid4()).replace("-", "_"),
        columns=[ColumnSetting(name=name, **info) for name, info in cols.items()],
    )

    in_data = [[2, 3]]
    processed_data = db._preprocess_data_for_insert(table_settings, in_data)

    assert len(processed_data[0]) == len(cols.keys()) - 1
    assert processed_data == in_data


def test_preprocess_data_for_insert_w_byte(db):
    cols = {
        "A": {"ctype": "INT", "is_primary": True},
        "B": {"ctype": "BYTEA"},
    }
    table_settings = TableSetting(
        name="table_from_info_" + str(uuid.uuid4()).replace("-", "_"),
        columns=[ColumnSetting(name=name, **info) for name, info in cols.items()],
    )

    in_data = [[2, "abcdefg".encode()]]
    processed_data = db._preprocess_data_for_insert(table_settings, in_data)
    exp_data = [[2, str(psycopg2.Binary("abcdefg".encode()))]]
    processed_data[0][1] = str(processed_data[0][1])

    assert len(processed_data[0]) == len(cols.keys())
    assert processed_data == exp_data


def test_preprocess_data_for_insert_w_byte_file(mocker, db):
    mocker.patch("builtins.open", mocker.mock_open(read_data="some_byte_str".encode()))
    from pathlib import Path

    mocker.patch.object(Path, "exists", return_value=True)
    cols = {
        "A": {"ctype": "INT", "is_primary": True},
        "B": {"ctype": "BYTEA"},
    }
    table_settings = TableSetting(
        name="table_from_info_" + str(uuid.uuid4()).replace("-", "_"),
        columns=[ColumnSetting(name=name, **info) for name, info in cols.items()],
    )

    in_data = [[2, "/path/to/file"]]
    processed_data = db._preprocess_data_for_insert(table_settings, in_data)
    exp_data = [[2, str(psycopg2.Binary("some_byte_str".encode()))]]
    processed_data[0][1] = str(processed_data[0][1])

    assert len(processed_data[0]) == len(cols.keys())
    assert processed_data == exp_data


def test_preprocess_data_for_insert_w_byte_error(db):
    cols = {
        "A": {"ctype": "INT", "is_primary": True},
        "B": {"ctype": "BYTEA"},
    }
    table_settings = TableSetting(
        name="table_from_info_" + str(uuid.uuid4()).replace("-", "_"),
        columns=[ColumnSetting(name=name, **info) for name, info in cols.items()],
    )

    in_data = [[2, "abcdefg"]]
    with pytest.raises(BinaryDataException):
        db._preprocess_data_for_insert(table_settings, in_data)


def test_preprocess_data_for_insert_invalid_data_error(db):
    cols = {
        "A": {"ctype": "INT", "is_primary": True},
        "B": {"ctype": "INT"},
    }
    table_settings = TableSetting(
        name="table_from_info_" + str(uuid.uuid4()).replace("-", "_"),
        columns=[ColumnSetting(name=name, **info) for name, info in cols.items()],
    )

    in_data = [[2, 2, 3]]
    with pytest.raises(InvalidDataException):
        db._preprocess_data_for_insert(table_settings, in_data)


def test_insert(db, test_table_create_drop):

    cols = {
        "id": {"ctype": "VARCHAR(255)", "is_primary": True},
        "col1": {"ctype": "FLOAT"},
        "col2": {"ctype": "FLOAT"},
    }
    table_settings = TableSetting(
        name=test_table_create_drop,
        columns=[ColumnSetting(name=name, **info) for name, info in cols.items()],
    )

    data = [["ABC", 2.5, 5.2]]
    exp_ids = ["A", "B", "C", "D", "ABC"]
    ids_pre_insert = db.query_to_df(
        f"""
        SELECT t.id
        FROM {test_table_create_drop} t
        """
    )

    success, _ = db.insert(table_settings, data)
    assert success

    ids_post_insert = db.query_to_df(
        f"""
        SELECT t.id
        FROM {test_table_create_drop} t
        """
    )

    assert not ids_pre_insert.equals(ids_post_insert)
    assert ids_post_insert.id.to_list() == exp_ids
