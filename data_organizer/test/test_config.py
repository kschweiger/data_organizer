import pytest
from dynaconf import LazySettings, ValidationError

from data_organizer.config import get_config


def test_get_config(monkeypatch):
    test_pw = "abcd"
    monkeypatch.setenv("CONFIGTEST_DB__PASSWORD", test_pw)
    config = get_config(
        "CONFIGTEST", secrets="", config_dir_base="data_organizer/test/conf"
    )

    assert isinstance(config, LazySettings)
    assert config.db.password == test_pw
    assert config.tables is None


def test_get_config_error_required_secret():
    with pytest.raises(ValidationError):
        get_config("CONFIGTEST", secrets="", config_dir_base="data_organizer/test/conf")


def test_get_config_table_good(monkeypatch):
    test_pw = "abcd"
    monkeypatch.setenv("CONFIGTEST_DB__PASSWORD", test_pw)
    config = get_config(
        "CONFIGTEST",
        secrets="",
        config_dir_base="data_organizer/test/conf",
        additional_configs=["test_table_good.toml"],
    )

    assert config.tables is not None


@pytest.mark.parametrize(
    "table_file_name",
    [
        "test_table_mandatory_missing.toml",
        "test_table_name_missing.toml",
        "test_table_unexp_key.toml",
        "test_table_unexp_key_type.toml",
        "test_table_wrong_type.toml",
    ],
)
def test_get_config_table_error(monkeypatch, table_file_name):
    test_pw = "abcd"
    monkeypatch.setenv("CONFIGTEST_DB__PASSWORD", test_pw)
    with pytest.raises(ValidationError):
        get_config(
            "CONFIGTEST",
            secrets="",
            config_dir_base="data_organizer/test/conf",
            additional_configs=[table_file_name],
        )
