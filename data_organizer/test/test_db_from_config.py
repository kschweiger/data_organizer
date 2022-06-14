import os
from unittest import mock

import pytest
from dynaconf import LazySettings
from sqlalchemy.engine import Engine

from data_organizer.config import get_config
from data_organizer.db.connection import Backend, DatabaseConnection
from data_organizer.db.model import get_table_setting_from_dict
from data_organizer.utils import init_logging

init_logging("DEBUG")

if os.getenv("PG_DEV_DB_USER") is None or os.getenv("PG_DEV_DB_PASSWORD") is None:
    raise RuntimeError("Set $PG_DEV_DB_USER and $PG_DEV_DB_PASSWORD")


@pytest.fixture(scope="session")
def config() -> LazySettings:
    with mock.patch.dict(
        os.environ,
        {
            "TESTCONFIG_DB__user": os.getenv("PG_DEV_DB_USER"),
            "TESTCONFIG_DB__password": os.getenv("PG_DEV_DB_PASSWORD"),
        },
    ):
        config = get_config(
            "TESTCONFIG",
            secrets="",
            config_dir_base="data_organizer/test/conf",
            additional_configs=["test_table_good.toml"],
        )
        return config


@pytest.fixture(scope="session")
def database(config):
    db = DatabaseConnection(**config.db.to_dict())
    yield db
    db.close()


def test_init_db(config):
    db = DatabaseConnection(**config.db.to_dict())

    assert db.is_valid
    assert isinstance(db.engine, Engine)

    assert isinstance(db.backend, Backend)

    db.close()


def test_create_table(config, database):
    database.create_table_from_table_info(
        [get_table_setting_from_dict(config["table_1"].to_dict())]
    )

    assert database.has_table(config["table_1"].name)

    database.engine.execute(f"DROP TABLE {config['table_1'].name}")
