import os
import uuid

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from data_organizer.db.connection import Backend, DatabaseConnection
from data_organizer.db.exceptions import QueryReturnedNoData, TableNotExists
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
    this_data = db.query(
        f"""
        SELECT *
        FROM {test_table_create_drop} t
        """
    )

    assert isinstance(this_data, pd.DataFrame)
    assert not this_data.empty

    with pytest.raises(QueryReturnedNoData):
        db.query(
            f"""
            SELECT *
            FROM {test_table_create_drop} t
            WHERE t.id = 'XYZ'
            """
        )


def test_insert_append(db, test_table_create_drop):
    data_to_insert = pd.DataFrame({"id": ["E"], "col1": [222.0], "col2": [222.0]})

    db.insert(
        table_name=test_table_create_drop, data=data_to_insert, if_exists="append"
    )

    inserted_data = db.query(
        f"""
        SELECT *
        FROM {test_table_create_drop} t
        WHERE t.id = 'E'
        """
    )
    assert data_to_insert.equals(inserted_data)

    all_data = db.query(
        f"""
        SELECT *
        FROM {test_table_create_drop} t
        """
    )

    assert not data_to_insert.equals(all_data)

    with pytest.raises(TableNotExists):
        db.insert(table_name="bogus_table", data=data_to_insert, if_exists="append")


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

    tables = db.query(
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
