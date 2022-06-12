import pytest

from data_organizer.db.model import ColumnInfo


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
    c = ColumnInfo(**init_dict)

    assert c.name == exp_name
    assert c.ctype == exp_ctype
    assert c.is_primary == exp_is_primary
    assert c.is_nullable == exp_is_nullable
    assert c.is_unique == exp_is_unique
