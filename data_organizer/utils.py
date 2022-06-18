import logging
from typing import Callable, Union

import click
import coloredlogs

logger = logging.getLogger(__name__)


def parse_level(thisLevel: Union[int, str]) -> int:
    if thisLevel == 20 or thisLevel == "INFO":
        return logging.INFO
    elif thisLevel == 10 or thisLevel == "DEBUG":
        return logging.DEBUG
    elif thisLevel == 30 or thisLevel == "WARNING":
        return logging.WARNING
    elif thisLevel == 40 or thisLevel == "ERROR":
        return logging.ERROR
    elif thisLevel == 50 or thisLevel == "CRITICAL":
        return logging.CRITICAL
    else:
        return logging.NOTSET


def init_logging(thisLevel: Union[int, str]) -> bool:
    """Helper function for setting up python logging"""
    log_format = "[%(asctime)s] %(name)-30s %(levelname)-8s %(message)s"
    coloredlogs.install(level=parse_level(thisLevel), fmt=log_format)
    return True


def init_logging_to_file(file_name: str, thisLevel: Union[int, str]) -> None:
    log_format = "[%(asctime)s] %(name)-30s %(levelname)-8s %(message)s"
    logging.basicConfig(
        filename=file_name, format=log_format, level=parse_level(thisLevel)
    )


def echo_and_log(log_func: Callable, text: str, *args) -> None:
    if args:
        click.echo(text % args)
        log_func(text, *args)
    else:
        click.echo(text)
        log_func(text)
