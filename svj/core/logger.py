import logging
from .termcolor import colored


LOGGER_FORMATTER = logging.Formatter(
    fmt = colored('[svj|%(levelname)s|%(asctime)s|%(module)s]:', 'yellow') + ' %(message)s'
    )
SUBPROCESS_LOGGER_FORMATTER = logging.Formatter(
    fmt = colored('[subprocess]:', 'red') + ' %(message)s'
    )

DEFAULT_LOGGER_NAME = 'root'
DEFAULT_SUBPROCESS_LOGGER_NAME = 'subprocess'


def setup_logger(name=DEFAULT_LOGGER_NAME):
    handler = logging.StreamHandler()
    handler.setFormatter(LOGGER_FORMATTER)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger


def setup_subprocess_logger(name=DEFAULT_SUBPROCESS_LOGGER_NAME):
    handler = logging.StreamHandler()
    handler.setFormatter(SUBPROCESS_LOGGER_FORMATTER)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger


def set_log_file(
        log_file,
        logger_name=DEFAULT_LOGGER_NAME,
        subprocess_logger_name=DEFAULT_SUBPROCESS_LOGGER_NAME
        ):
    """
    Also outputs all logging to a file, but keeps the output
    to stderr as well.
    """
    log_file = osp.abspath(log_file)

    logger = logging.getLogger(logger_name)
    subprocess_logger = logging.getLogger(subprocess_logger_name)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(LOGGER_FORMATTER)
    logger.addHandler(file_handler)

    # Little bit dangerous; not sure whether logging will open the same file twice
    subprocess_file_handler = logging.FileHandler(log_file)
    subprocess_file_handler.setFormatter(SUBPROCESS_LOGGER_FORMATTER)
    subprocess_logger.addHandler(subprocess_file_handler)








