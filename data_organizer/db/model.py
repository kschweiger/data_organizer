from typing import Dict, List, Union

from pydantic import BaseModel

ColumnConfigType = Dict[str, Union[str, bool]]


class ColumnSetting(BaseModel):
    """Object holding information on columns"""

    name: str
    ctype: str
    is_primary: bool = False
    is_nullable: bool = False
    is_unique: bool = False


class TableSetting(BaseModel):
    """Object holding the information for a table"""

    name: str
    columns: List[ColumnSetting]


def get_table_setting_from_dict(
    data: Dict[str, Union[str, ColumnConfigType]]
) -> TableSetting:
    """
    Create a TableSetting object from a dict. Intended as bridge from the config to
    a validated object (with defaults) that can be used with the DatabaseConnection
    object.

    Args:
        data: Table info as dict. See config for tables for more information on
              structure.

    Returns: A validated TableSetting object wiht defaults.
    """
    column_data: Dict[str, ColumnConfigType] = {
        key: item for key, item in data.items() if key != "name"  # type: ignore
    }

    return TableSetting(
        name=data["name"],
        columns=[
            ColumnSetting(name=key, **items) for key, items in column_data.items()
        ],
    )
