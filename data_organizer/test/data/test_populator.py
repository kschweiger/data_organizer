from dataclasses import dataclass
from typing import List
from unittest.mock import MagicMock

import pytest

from data_organizer.data.populator import get_table_data_from_user_input


@dataclass
class MockColumnInfo:
    name: str
    ctype: str
    is_nullable: bool


@dataclass
class MockTableInfo:
    columns: List[MockColumnInfo]


@pytest.mark.parametrize(
    ("test_input", "test_ctype", "test_is_nullable", "exp_result"),
    [
        ("1", "INT", False, 1),
        ("1", "Float", False, 1.0),
        ("1", "FLOAT", False, 1.0),
        ("1.2", "FLOAT", False, 1.2),
        ("2022-01-01", "DATE", False, "2022-01-01"),
        ("11:22:33", "TIME", False, "11:22:33"),
        ("11:22", "TIME", False, "11:22"),
        ("Some String", "VARCAR(30)", False, "Some String"),
        ("", "VARCAR(30)", True, None),
        ("", "INT", True, None),
        ("", "FLOAT", True, None),
        ("", "TIME", True, None),
        ("", "DATE", True, None),
    ],
)
def test_get_table_data_from_user_input(
    test_input, test_ctype, test_is_nullable, exp_result
):
    mock_config = MagicMock()
    mock_config.tables = {
        "tab": MockTableInfo(
            [MockColumnInfo("TestColumn", test_ctype, test_is_nullable)]
        )
    }

    mock_prompt_func = MagicMock()
    mock_prompt_func.return_value = test_input

    result = get_table_data_from_user_input(
        mock_config, "tab", mock_prompt_func, debug=True
    )

    assert exp_result == result["TestColumn"].iloc[0]


@pytest.mark.parametrize(
    ("test_input", "test_ctype"),
    [
        ("1.2", "INT"),
        ("AAA", "INT"),
        ("AAA", "FLOAT"),
        ("2022_01_01", "DATE"),
        ("2022,01,01", "DATE"),
        ("2022-01", "DATE"),
        ("2022-01-41", "DATE"),  # invalid day
        ("2022-13-01", "DATE"),  # invalid month
        ("11", "TIME"),  # HH format is fine for datetime but not for PG
        ("25:00:00", "TIME"),  # invalid Hour
        ("10:75:00", "TIME"),  # invalid Minutes
        ("10:10:69", "TIME"),  # invalid Seconds
        ("Some String", "TIME"),
    ],
)
def test_get_table_data_from_user_input_value_error(test_input, test_ctype):
    mock_config = MagicMock()
    mock_config.tables = {
        "tab": MockTableInfo([MockColumnInfo("TestColumn", test_ctype, False)])
    }

    mock_prompt_func = MagicMock()
    mock_prompt_func.return_value = test_input

    with pytest.raises(ValueError):  # noqa: PT011
        get_table_data_from_user_input(mock_config, "tab", mock_prompt_func, debug=True)
