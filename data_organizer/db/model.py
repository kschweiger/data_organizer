from typing import List

from pydantic import BaseModel


class ColumnInfo(BaseModel):
    """Object holding information on columns"""

    name: str
    ctype: str
    is_primary: bool = False
    is_nullable: bool = False
    is_unique: bool = False


class TableInfo(BaseModel):
    """Object holding the information for a table"""

    name: str
    columns: List[ColumnInfo]
