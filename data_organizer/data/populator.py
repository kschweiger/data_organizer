import logging
from typing import Callable, Dict, List, Union

import pandas as pd

from data_organizer.config import OrganizerConfig

logger = logging.getLogger(__name__)


def get_table_data_from_user_input(
    config: OrganizerConfig, table: str, prompt_func: Callable[[str], str] = input
) -> pd.DataFrame:
    """
    Get user input for all columns in the table.

    Args:
        prompt_func: Function used to get the user input. E.g. input()
        config: OrganizerConfig object with tables
        table: Name of the table

    Returns: User inputted data in DataFrame
    """

    data: Dict[str, List[Union[None, str, float, int]]] = {}
    column_input: Union[None, str, float, int]
    for column in config.tables[table].columns:
        exit_input = False
        while not exit_input:
            try:
                column_input = prompt_func("Input value for %s: " % column.name)
                if column.is_nullable and column_input == "":
                    column_input = None
                elif column.ctype == "INT":
                    column_input = int(column_input)
                elif column.ctype == "FLOAT":
                    column_input = float(column_input)
                else:
                    column_input = str(column_input)
                data[column.name] = [column_input]
                exit_input = True
            except ValueError:
                logger.error("Invalid input for column with typ %s", column.ctype)
    return pd.DataFrame(data)
