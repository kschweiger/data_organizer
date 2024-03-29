from dataclasses import make_dataclass
from typing import Dict, List, Optional, Union

from pydantic import BaseModel

ColumnConfigType = Dict[str, Union[str, bool]]


class ColumnSetting(BaseModel):
    """Object holding information on columns"""

    name: str
    ctype: str
    is_primary: bool = False
    is_nullable: bool = False
    is_unique: bool = False
    is_inserted: bool = True
    default: Optional[str] = None

    @property
    def typed_default(self):
        if self.default is None:
            return None
        if self.ctype == "INT":
            return int(self.default)
        elif self.ctype == "FLOAT":
            return float(self.default)
        else:
            return self.default

    @property
    def get_py_type(self):

        if self.ctype in ["INT", "SERIAL"]:
            base_type = int
        elif self.ctype == "FLOAT":
            base_type = float
        elif self.ctype.startswith("VARCHAR") or self.ctype == "TEXT":
            base_type = str
        else:
            base_type = None

        if base_type is not None and self.is_nullable:
            return Optional[base_type]
        else:
            return base_type


class TableSetting(BaseModel):
    """Object holding the information for a table"""

    name: str
    columns: List[ColumnSetting]
    rel_table: Optional[str] = None
    rel_table_common_column: Optional[str] = None
    rel_table_common_column_as_foreign_key: bool = False
    disable_auto_insert_columns: bool = False

    @property
    def dataclass(self):
        fields = []
        for column in self.columns:
            column_name = column.name
            py_type = column.get_py_type
            if py_type is None:
                fields.append(column_name)
            else:
                fields.append((column_name, py_type))

        return make_dataclass(cls_name=self.name, fields=fields)


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
        key: item  # type: ignore
        for key, item in data.items()
        if isinstance(data[key], dict)
    }

    return TableSetting(
        name=data["name"],
        rel_table=data["rel_table"] if "rel_table" in data.keys() else None,
        rel_table_common_column=(
            data["rel_table_common_column"]
            if "rel_table_common_column" in data.keys()
            else None
        ),
        columns=[
            ColumnSetting(name=key, **items) for key, items in column_data.items()
        ],
    )
