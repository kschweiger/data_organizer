import pytest
from dynaconf import LazySettings, ValidationError

from data_organizer.config import OrganizerConfig, get_settings
from data_organizer.db.model import TableSetting


def test_get_config(monkeypatch):
    test_pw = "abcd"
    monkeypatch.setenv("CONFIGTEST_DB__PASSWORD", test_pw)
    config = get_settings(
        "CONFIGTEST", config_dir_base="data_organizer/test/conf", secrets=""
    )

    assert isinstance(config, LazySettings)
    assert config.db.password == test_pw
    assert config.tables is None


def test_get_config_error_required_secret():
    with pytest.raises(ValidationError):
        get_settings(
            "CONFIGTEST", config_dir_base="data_organizer/test/conf", secrets=""
        )


def test_get_config_table_good(monkeypatch):
    test_pw = "abcd"
    monkeypatch.setenv("CONFIGTEST_DB__PASSWORD", test_pw)
    config = get_settings(
        "CONFIGTEST",
        config_dir_base="data_organizer/test/conf",
        secrets="",
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
        "test_table_rel_not_defined.toml",
        "test_table_common_not_defined.toml",
        "test_table_common_not_valid.toml",
    ],
)
def test_get_config_table_error(monkeypatch, table_file_name):
    test_pw = "abcd"
    monkeypatch.setenv("CONFIGTEST_DB__PASSWORD", test_pw)
    with pytest.raises(ValidationError):
        get_settings(
            "CONFIGTEST",
            config_dir_base="data_organizer/test/conf",
            secrets="",
            additional_configs=[table_file_name],
        )


def test_organizer_config_tables(monkeypatch):
    test_pw = "abcd"
    monkeypatch.setenv("CONFIGTEST_DB__PASSWORD", test_pw)
    config = OrganizerConfig(
        "CONFIGTEST",
        config_dir_base="data_organizer/test/conf",
        secrets="",
        additional_configs=["test_table_good.toml"],
    )

    assert isinstance(config.tables, dict)
    assert len(config.tables.keys()) >= 1
    for _, info in config.tables.items():
        assert isinstance(info, TableSetting)


def test_organizer_rel_tables(monkeypatch):
    test_pw = "abcd"
    monkeypatch.setenv("CONFIGTEST_DB__PASSWORD", test_pw)
    config = OrganizerConfig(
        "CONFIGTEST",
        config_dir_base="data_organizer/test/conf",
        secrets="",
        additional_configs=["test_table_good_w_rel.toml"],
    )

    for _, info in config.tables.items():
        assert isinstance(info, TableSetting)
