import logging
from datetime import date, time
from typing import Callable, Dict, List, Tuple, Union

import pandas as pd

from data_organizer.config import OrganizerConfig

logger = logging.getLogger(__name__)

column_input_type = Union[None, str, float, int]


def get_table_data_from_user_input(
    config: OrganizerConfig,
    table: str,
    prompt_func: Callable[..., str] = input,
    set_values: Dict[str, Union[str, int, float]] = {},
    use_prompt_default_arg: bool = True,
    debug: bool = False,
) -> Tuple[List[str], List[column_input_type]]:
    columns: List[str] = []
    values: List[column_input_type] = []
    column_input: column_input_type
    for column in config.tables[table].columns:
        exit_input = False
        add_err_info = None
        if column.ctype.upper() in config.settings.table_settings.auto_fill_ctypes:
            continue
        while not exit_input:
            try:
                prompt_text = "Input value for %s: " % column.name
                if column.default is not None and not use_prompt_default_arg:
                    prompt_text = "Input value for %s (Default: %s): " % (
                        column.name,
                        column.default,
                    )
                column_input_prompt: str
                if column.name in set_values.keys():
                    column_input_prompt = str(set_values[column.name])
                else:
                    if use_prompt_default_arg and column.default is not None:
                        column_input_prompt = prompt_func(
                            prompt_text, default=column.default
                        )
                    else:
                        column_input_prompt = prompt_func(prompt_text)
                if column.is_nullable and (
                    column_input_prompt == ""
                    or column_input_prompt.upper() == "NULL"
                    or column_input_prompt.upper() == "NONE"
                ):
                    column_input = None
                elif column.default is not None and column_input_prompt == "":
                    column_input = column.typed_default
                elif column.ctype.upper() == "INT":
                    column_input = int(column_input_prompt)
                elif column.ctype.upper() == "FLOAT":
                    column_input = float(column_input_prompt)
                else:
                    # For some types additional checks should be executed
                    column_input = str(column_input_prompt)
                    if column.ctype.upper() == "DATE":
                        date.fromisoformat(column_input)
                        add_err_info = "Use YYYY-MM-DD (iso format) for DATE columns"
                    elif column.ctype.upper() == "TIME":
                        time.fromisoformat(column_input)
                        add_err_info = (
                            "Use HH:MM, or HH:MM:SS (iso format) for TIME columns"
                        )
                        if len(column_input) == 2:
                            raise ValueError("HH format not supported")
                    elif column.ctype.upper() == "INTERVAL":
                        add_err_info = "Only HH:MM:SS or HH:MM formats are supported "
                        column_input = column_input.replace(" ", "")
                        if ":" not in column_input:
                            raise ValueError
                        elems = column_input.split(":")
                        if not (len(elems) == 2 or len(elems) == 3):
                            raise ValueError
                        for i, elem in enumerate(elems):
                            # Raise value error if element can not be converted to int
                            if elem == "":
                                add_err_info = "Element %s is empty" % i
                                raise ValueError

                            int_elem = int(elem)
                            if i != 0:
                                if int_elem >= 60:
                                    add_err_info = "Element %s must be < 59" % i
                                    raise ValueError

                    else:
                        pass
                columns.append(column.name)
                values.append(column_input)
                exit_input = True
            except ValueError as e:
                logger.error("Invalid input for column with typ %s", column.ctype)
                if add_err_info is not None:
                    logger.error(add_err_info)
                if debug:
                    raise e
    logger.debug("Read VALUES %s", values)
    return columns, values


def get_table_data_df_from_user_input(
    config: OrganizerConfig,
    table: str,
    prompt_func: Callable[..., str] = input,
    set_values: Dict[str, Union[str, int, float]] = {},
    use_prompt_default_arg: bool = True,
    debug: bool = False,
) -> pd.DataFrame:
    """
    Get user input for all columns in the table.

    Args:
        prompt_func: Function used to get the user input. E.g. input()
        config: OrganizerConfig object with tables
        table: Name of the table
        set_values: Pre-set values that would be prompted otherwise
        use_prompt_default_arg: Flag determining if the *default* arg should be passed
                               to the prompt_func. No check is made to make sure this
                               is valid.
        debug: Flag enabling a strict error checking mode that will raise the ValueError

    Returns: User inputted data in DataFrame
    """
    columns, values = get_table_data_from_user_input(
        config=config,
        table=table,
        prompt_func=prompt_func,
        set_values=set_values,
        use_prompt_default_arg=use_prompt_default_arg,
        debug=debug,
    )

    data: Dict[str, List[column_input_type]] = {}
    for column, value in zip(columns, values):
        data[column] = [value]

    return pd.DataFrame(data)
