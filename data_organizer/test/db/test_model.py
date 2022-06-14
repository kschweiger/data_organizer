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
    ),
    [
        ({"name": "A", "ctype": "FLOAT"}, "A", "FLOAT", False, False, False),
        (
            {"name": "A", "ctype": "FLOAT", "is_primary": True},
            "A",
            "FLOAT",
            True,
            False,
            False,
        ),
        (
            {"name": "A", "ctype": "FLOAT", "is_nullable": True},
            "A",
            "FLOAT",
            False,
            True,
            False,
        ),
        (
            {"name": "A", "ctype": "FLOAT", "is_primary": True, "is_nullable": True},
            "A",
            "FLOAT",
            True,
            True,
            False,
        ),
        (
            {"name": "A", "ctype": "FLOAT", "is_unique": True},
            "A",
            "FLOAT",
            False,
            False,
            True,
        ),
    ],
)
def test_column_info(
    init_dict, exp_name, exp_ctype, exp_is_primary, exp_is_nullable, exp_is_unique
):
    c = ColumnSetting(**init_dict)

    assert c.name == exp_name
    assert c.ctype == exp_ctype
    assert c.is_primary == exp_is_primary
    assert c.is_nullable == exp_is_nullable
    assert c.is_unique == exp_is_unique


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
