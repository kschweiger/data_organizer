import pytest

from data_organizer.db.model import (
    ColumnSetting,
    TableSetting,
    get_table_setting_from_dict,
)


@pytest.mark.parametrize(
    (
        "init_dict",
        "exp_name",
        "exp_ctype",
        "exp_is_primary",
        "exp_is_nullable",
        "exp_is_unique",
        "exp_default",
        "exp_typed_default",
    ),
    [
        (
            {"name": "A", "ctype": "FLOAT"},
            "A",
            "FLOAT",
            False,
            False,
            False,
            None,
            None,
        ),
        (
            {"name": "A", "ctype": "FLOAT", "is_primary": True},
            "A",
            "FLOAT",
            True,
            False,
            False,
            None,
            None,
        ),
        (
            {"name": "A", "ctype": "FLOAT", "is_nullable": True},
            "A",
            "FLOAT",
            False,
            True,
            False,
            None,
            None,
        ),
        (
            {"name": "A", "ctype": "FLOAT", "is_primary": True, "is_nullable": True},
            "A",
            "FLOAT",
            True,
            True,
            False,
            None,
            None,
        ),
        (
            {"name": "A", "ctype": "FLOAT", "is_unique": True},
            "A",
            "FLOAT",
            False,
            False,
            True,
            None,
            None,
        ),
        (
            {"name": "A", "ctype": "FLOAT", "default": "1.2"},
            "A",
            "FLOAT",
            False,
            False,
            False,
            "1.2",
            1.2,
        ),
        (
            {"name": "A", "ctype": "FLOAT", "default": "2"},
            "A",
            "FLOAT",
            False,
            False,
            False,
            "2",
            2.0,
        ),
        (
            {"name": "A", "ctype": "INT", "default": 2},
            "A",
            "INT",
            False,
            False,
            False,
            "2",
            2,
        ),
        (
            {"name": "A", "ctype": "VARCAR(20)", "default": "test_value"},
            "A",
            "VARCAR(20)",
            False,
            False,
            False,
            "test_value",
            "test_value",
        ),
    ],
)
def test_column_info(
    init_dict,
    exp_name,
    exp_ctype,
    exp_is_primary,
    exp_is_nullable,
    exp_is_unique,
    exp_default,
    exp_typed_default,
):
    c = ColumnSetting(**init_dict)

    assert c.name == exp_name
    assert c.ctype == exp_ctype
    assert c.is_primary == exp_is_primary
    assert c.is_nullable == exp_is_nullable
    assert c.is_unique == exp_is_unique
    assert c.default == exp_default
    assert c.typed_default == exp_typed_default


def test_get_table_setting_from_dict():
    test_dict = {
        "name": "table_name",
        "A": {"ctype": "INT", "is_primary": True, "is_unique": True},
        "B": {"ctype": "INT", "is_primary": True},
        "C": {"ctype": "FLOAT"},
        "D": {"ctype": "FLOAT", "nullable": False},
    }

    table_setting = get_table_setting_from_dict(test_dict)

    assert isinstance(table_setting, TableSetting)
