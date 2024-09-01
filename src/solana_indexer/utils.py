import logging
from colorlog import ColoredFormatter
import os

def setup_logger():
    logger = logging.getLogger("main_logger")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        color_scheme = {
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red,bg_white",
        }
        formatter = ColoredFormatter(
            "%(log_color)s%(asctime)s - %(levelname)s - %(module)s - %(message)s",
            datefmt=None,
            reset=True,
            log_colors=color_scheme,
            secondary_log_colors={},
            style="%",
        )
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    return logger


# Create a single instance of the logger
logger = setup_logger()