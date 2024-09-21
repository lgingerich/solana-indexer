import asyncio
from colorlog import ColoredFormatter
from functools import wraps
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import random
import yaml
from solana.exceptions import SolanaRpcException

def load_config(filename='config.yml'):
    project_root = Path(__file__).resolve().parent.parent.parent
    config_path = project_root / filename
    if config_path.is_file():
        with open(config_path) as f:
            return yaml.safe_load(f)
    raise FileNotFoundError(f"Config file '{filename}' not found in project root")

def setup_logger(log_file_path="logs/solana_indexer.log", log_level=logging.INFO):
    logger = logging.getLogger("main_logger")
    if not logger.handlers:
        logger.setLevel(log_level)

        # Console handler with color
        color_scheme = {
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red,bg_white",
        }
        console_formatter = ColoredFormatter(
            "%(log_color)s%(asctime)s - %(levelname)s - %(module)s - %(message)s",
            datefmt=None,
            reset=True,
            log_colors=color_scheme,
            secondary_log_colors={},
            style="%",
        )
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        # File handler
        log_dir = os.path.dirname(log_file_path)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

        file_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(module)s - %(message)s"
        )
        file_handler = RotatingFileHandler(
            log_file_path, maxBytes=10 * 1024 * 1024, backupCount=5
        )  # 10MB file size
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger


# Create a single instance of the logger
logger = setup_logger()


def async_retry(
    retries=3,
    base_delay=1,
    exponential_backoff=True,
    jitter=True,
    exceptions=(SolanaRpcException,),
):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(1, retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt == retries:
                        logger.error(
                            f"All retry attempts failed for {func.__name__}: {str(e)}"
                        )
                        raise

                    delay = (
                        base_delay * (2 ** (attempt - 1))
                        if exponential_backoff
                        else base_delay
                    )
                    if jitter:
                        delay *= random.uniform(1.0, 1.5)

                    logger.warning(
                        f"Attempt {attempt} failed for {func.__name__}. Retrying in {delay:.2f} seconds. Error: {str(e)}"
                    )
                    await asyncio.sleep(delay)

        return wrapper

    return decorator
