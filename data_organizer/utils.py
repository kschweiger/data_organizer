import logging
from typing import Union

import coloredlogs

logger = logging.getLogger(__name__)


def init_logging(thisLevel: Union[int, str]) -> bool:
    """Helper function for setting up python logging"""
    log_format = "[%(asctime)s] %(name)-30s %(levelname)-8s %(message)s"
    if thisLevel == 20 or thisLevel == "INFO":
        thisLevel = logging.INFO
    elif thisLevel == 10 or thisLevel == "DEBUG":
        thisLevel = logging.DEBUG
    elif thisLevel == 30 or thisLevel == "WARNING":
        thisLevel = logging.WARNING
    elif thisLevel == 40 or thisLevel == "ERROR":
        thisLevel = logging.ERROR
    elif thisLevel == 50 or thisLevel == "CRITICAL":
        thisLevel = logging.CRITICAL
    else:
        thisLevel = logging.NOTSET

    coloredlogs.install(level=thisLevel, fmt=log_format)
    return True
