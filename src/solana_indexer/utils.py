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
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Confirmed

# Function to load and validate configuration from a YAML file
async def load_config(filename="config.yml"):
    """
    Load and validate configuration from a YAML file.

    :param filename: str, name of the configuration file
    :return: dict, validated configuration dictionary
    """
    try:
        # Find the project root directory and construct the config file path
        project_root = Path(__file__).resolve().parent.parent.parent
        config_path = project_root / filename
        
        # Check if the config file exists
        if not config_path.is_file():
            raise FileNotFoundError(f"Config file \"{filename}\" not found in project root")
        
        # Load the YAML configuration
        with open(config_path) as f:
            config = yaml.safe_load(f)
        
        # Validate required configuration keys
        if "rpc" not in config or "url" not in config["rpc"]:
            raise ValueError("Invalid config: 'rpc.url' is required")
        
        if "indexer" not in config or "start_slot" not in config["indexer"]:
            raise ValueError("Invalid config: 'indexer.start_slot' is required")

        # TO DO: Remove the redundancy of this setup — I have a function elsewhere to get the latest slot
        # Get the latest slot from the Solana network
        rpc_url = config["rpc"]["url"]
        async with AsyncClient(rpc_url) as client:
            try:
                latest_slot = (await client.get_slot(Confirmed)).value
            except SolanaRpcException as e:
                logger.error(f"Failed to get latest slot: {str(e)}")
                raise
        
        # Helper function to process and validate slot values
        def process_slot(value, slot_type):
            if isinstance(value, int) and value >= 0:
                return value
            if value is None and slot_type == "end_slot":
                return None
            if isinstance(value, str):
                if value.lower() == "genesis":
                    return 0
                if value.lower() == "latest":
                    return latest_slot
                # TO DO: Add handling to determine which block/slot was last processed and saved
                if value.lower() == "last_processed":
                    pass
            raise ValueError(f"Invalid {slot_type} value. Must be a positive integer, null (for end_slot), 'genesis', or 'latest'")
        
        # Process and validate start_slot and end_slot values
        config["indexer"]["start_slot"] = process_slot(config["indexer"]["start_slot"], "start_slot")
        if "end_slot" in config["indexer"]:
            config["indexer"]["end_slot"] = process_slot(config["indexer"]["end_slot"], "end_slot")
        
        return config
    
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML config: {str(e)}")
        raise
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
