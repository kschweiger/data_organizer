from dataclasses import dataclass
from typing import List, Optional
from unittest.mock import MagicMock

import pytest

from data_organizer.data.populator import get_table_data_df_from_user_input


@dataclass
class MockColumnInfo:
    name: str
    ctype: str
    is_nullable: bool
    default: Optional[str]


@dataclass
class MockTableInfo:
    columns: List[MockColumnInfo]


@pytest.mark.parametrize(
    ("test_input", "test_ctype", "test_is_nullable", "test_default", "exp_result"),
    [
        ("1", "INT", False, None, 1),
        ("1", "Float", False, None, 1.0),
        ("1", "FLOAT", False, None, 1.0),
        ("1.2", "FLOAT", False, None, 1.2),
        ("2022-01-01", "DATE", False, None, "2022-01-01"),
        ("11:22:33", "TIME", False, None, "11:22:33"),
        ("11:22", "TIME", False, None, "11:22"),
        ("11:22:33", "INTERVAL", False, None, "11:22:33"),
        ("11:22", "INTERVAL", False, None, "11:22"),
        ("Some String", "VARCAR(30)", False, None, "Some String"),
        # "" can be returned by input but not by click.prompt
        ("", "VARCAR(30)", True, None, None),
        ("", "INT", True, None, None),
        ("", "FLOAT", True, None, None),
        ("", "TIME", True, None, None),
        ("", "DATE", True, None, None),
        ("NULL", "VARCAR(30)", True, None, None),
        ("NONE", "VARCAR(30)", True, None, None),
    ],
)
def test_get_table_data_from_user_input(
    test_input, test_ctype, test_is_nullable, test_default, exp_result
):
    mock_config = MagicMock()
    mock_config.tables = {
        "tab": MockTableInfo(
            [MockColumnInfo("TestColumn", test_ctype, test_is_nullable, test_default)]
        )
    }

    mock_prompt_func = MagicMock()
    mock_prompt_func.return_value = test_input

    result = get_table_data_df_from_user_input(
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
        ("Some String", "INTERVAL"),
        ("11:69:11", "INTERVAL"),  # MM > 59
        ("11:69", "INTERVAL"),  # MM > 59
        ("11:11:69", "INTERVAL"),  # SS > 59
        ("110251", "INTERVAL"),  # No : in input
        ("11:11:", "INTERVAL"),  # element 1 is empty str
        ("11:", "INTERVAL"),  # element 1 is empty str
        ("11:11:11:21", "INTERVAL"),  # SS > 59
    ],
)
def test_get_table_data_from_user_input_value_error(test_input, test_ctype):
    mock_config = MagicMock()
    mock_config.tables = {
        "tab": MockTableInfo([MockColumnInfo("TestColumn", test_ctype, False, None)])
    }

    mock_prompt_func = MagicMock()
    mock_prompt_func.return_value = test_input

    with pytest.raises(ValueError):  # noqa: PT011
        get_table_data_df_from_user_input(
            mock_config, "tab", mock_prompt_func, debug=True
        )


def test_get_table_data_from_user_input_pre_set():
    mock_config = MagicMock()
    mock_config.tables = {
        "tab": MockTableInfo([MockColumnInfo("TestColumn", "INT", False, None)])
    }

    mock_prompt_func = MagicMock()

    result = get_table_data_df_from_user_input(
        mock_config, "tab", mock_prompt_func, set_values={"TestColumn": 200}, debug=True
    )

    assert mock_prompt_func.call_count == 0
    assert result["TestColumn"].iloc[0] == 200
