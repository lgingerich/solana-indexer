import asyncio
from colorlog import ColoredFormatter
from dynaconf import Dynaconf, Validator
from functools import wraps
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import random

from solana.exceptions import SolanaRpcException
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Confirmed

# Initialize Dynaconf
project_root = Path(__file__).resolve().parent.parent.parent
settings = Dynaconf(
    envvar_prefix="SOLANA_INDEXER",
    settings_files=[project_root / "config.yml", project_root / ".secrets.yml"],
    environments=True,
    load_dotenv=True,
)

# Function to load and validate configuration
async def load_config():
    """
    Load and validate configuration using Dynaconf.

    :return: Dynaconf settings object
    """
    try:
        # Validate required configuration keys
        settings.validators.register(
            Validator('environment', must_exist=True, is_in=['development', 'production']),
            Validator('rpc.url', must_exist=True, is_type_of=str),
            Validator('indexer.start_slot', must_exist=True),
            # Validator('indexer.end_slot', must_exist=False, is_type_of=(int, str, type(None))),
            Validator('indexer.end_slot', must_exist=True),
            Validator('data_store.type', must_exist=True, is_in=['parquet', 'iceberg', 'spark']),
            # Parquet-specific validations
            Validator('data_store.params.base_path', must_exist=True, when=Validator('data_store.type', eq='parquet')),
            # Iceberg-specific validations
            Validator('data_store.params.warehouse_path', must_exist=True, when=Validator('data_store.type', eq='iceberg')),
            Validator('data_store.params.catalog_name', must_exist=True, when=Validator('data_store.type', eq='iceberg')),
            Validator('data_store.params.namespace', must_exist=True, when=Validator('data_store.type', eq='iceberg')),
            Validator('data_store.params.uri', must_exist=True, when=Validator('data_store.type', eq='iceberg')),
            # Spark-specific validations
            Validator('data_store.params.warehouse_path', must_exist=True, when=Validator('data_store.type', eq='spark')),
            Validator('data_store.params.catalog_name', must_exist=True, when=Validator('data_store.type', eq='spark')),
            Validator('data_store.params.namespace', must_exist=True, when=Validator('data_store.type', eq='spark')),
            Validator('data_store.params.spark_config', must_exist=True, when=Validator('data_store.type', eq='spark')),
            # Table selection validations
            Validator('tables.blocks', must_exist=True, is_type_of=bool),
            Validator('tables.transactions', must_exist=True, is_type_of=bool),
            Validator('tables.instructions', must_exist=True, is_type_of=bool),
            Validator('tables.rewards', must_exist=True, is_type_of=bool),
        )
        settings.validators.validate()

        # Get the latest slot from the Solana network
        rpc_url = settings.rpc.url
        async with AsyncClient(rpc_url) as client:
            try:
                latest_slot = (await client.get_slot(Confirmed)).value
            except SolanaRpcException as e:
                logger.error(f"Failed to get latest slot: {str(e)}")
                raise

        # # Helper function to process and validate slot values
        def process_slot(value, slot_type):
            if isinstance(value, int) and value >= 0:
                return value
            if value is None and slot_type == "end_slot":
                return None
            if isinstance(value, str):
                if value.lower() in ["genesis", "latest", "last_processed"]:
                    return value.lower()
            raise ValueError(f"Invalid {slot_type} value. Must be a positive integer, null (for end_slot), 'genesis', 'latest', or 'last_processed'")


        # Process and validate start_slot and end_slot values
        settings.indexer.start_slot = process_slot(settings.indexer.start_slot, "start_slot")
        if "end_slot" in settings.indexer:
            settings.indexer.end_slot = process_slot(settings.indexer.end_slot, "end_slot")

        # Process table selection
        settings.tables_to_save = [
            table for table, enabled in settings.tables.items() if enabled
        ]

        return settings

    except Exception as e:
        logger.error(f"Unexpected error in load_config: {str(e)}")
        raise

# Function to set up a logger with both console and file handlers
def setup_logger(log_file_path="logs/solana_indexer.log", log_level=logging.INFO):
    """
    Set up a logger with both console and file handlers.

    :param log_file_path: str, path to the log file
    :param log_level: int, logging level
    :return: logging.Logger, configured logger instance
    """
    logger = logging.getLogger("main_logger")
    if not logger.handlers:
        logger.setLevel(log_level)

        # Console handler with color-coded output
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

        # File handler for persistent logging
        log_dir = os.path.dirname(log_file_path)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

        file_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(module)s - %(message)s"
        )
        file_handler = RotatingFileHandler(
            log_file_path, maxBytes=10 * 1024 * 1024, backupCount=5
        )  # 10MB file size with 5 backup files
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger

# Create a single instance of the logger to be used throughout the application
logger = setup_logger()

# Decorator for implementing retry logic with exponential backoff for async functions
def async_retry(
    retries=3,
    base_delay=1,
    exponential_backoff=True,
    jitter=True,
    exceptions=(SolanaRpcException,),
):
    """
    Decorator for implementing retry logic with exponential backoff for async functions.

    :param retries: int, number of retry attempts
    :param base_delay: int, base delay between retries in seconds
    :param exponential_backoff: bool, whether to use exponential backoff
    :param jitter: bool, whether to add random jitter to the delay
    :param exceptions: tuple, exceptions to catch and retry
    :return: function, decorated function
    """
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
                except Exception as e:
                    logger.error(f"Unexpected error in {func.__name__}: {str(e)}")
                    raise

        return wrapper

    return decorator
