import logging
from typing import Callable, Tuple, Union

import click
import coloredlogs

logger = logging.getLogger(__name__)


def parse_level(this_level: Union[int, str]) -> Tuple[int, Callable]:
    if this_level == 20 or this_level == "INFO":
        return logging.INFO, logging.info
    elif this_level == 10 or this_level == "DEBUG":
        return logging.DEBUG, logging.debug
    elif this_level == 30 or this_level == "WARNING":
        return logging.WARNING, logging.warning
    elif this_level == 40 or this_level == "ERROR":
        return logging.ERROR, logging.error
    elif this_level == 50 or this_level == "CRITICAL":
        return logging.CRITICAL, logging.critical
    else:
        raise RuntimeError("%s is not supported" % this_level)


def init_logging(this_level: Union[int, str]) -> bool:
    """Helper function for setting up python logging"""
    log_format = "[%(asctime)s] %(name)-35s %(levelname)-8s %(message)s"
    level, _ = parse_level(this_level)
    coloredlogs.install(level=level, fmt=log_format)
    return True


def init_logging_to_file(file_name: str, thisLevel: Union[int, str]) -> None:
    log_format = "[%(asctime)s] %(name)-35s %(levelname)-8s %(message)s"
    level, _ = parse_level(thisLevel)
    logging.basicConfig(filename=file_name, format=log_format, level=level)


def echo_and_log(level: Union[int, str], text: str, *args) -> None:
    _, log_func = parse_level(level)
    if args:
        click.echo(text % args)
        log_func(text, *args)
    else:
        click.echo(text)
        log_func(text)
