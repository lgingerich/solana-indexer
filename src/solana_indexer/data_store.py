import os
from pathlib import Path
import polars as pl
from typing import Optional
from utils import logger

def write_df_to_parquet(df: pl.DataFrame, data_type: str, slot: int, base_path: str = "data"):
    """
    Write a Polars DataFrame to a Parquet file.
    
    This function organizes data into a directory structure based on the data type and slot number,
    creating a scalable and easily navigable storage system for blockchain data.
    
    :param df: pl.DataFrame, the DataFrame to write
    :param data_type: str, the type of data (e.g., 'blocks', 'transactions', 'instructions', 'rewards')
    :param slot: int, the slot number for this data
    :param base_path: str, the base directory to store the Parquet files
    :return: str, the path of the written Parquet file
    """
    try:
        # Validate input parameters
        if not isinstance(df, pl.DataFrame):
            raise ValueError("df must be a Polars DataFrame")
        if not isinstance(data_type, str) or not data_type:
            raise ValueError("data_type must be a non-empty string")
        if not isinstance(slot, int) or slot < 0:
            raise ValueError("slot must be a non-negative integer")
        
        # Create a directory structure that groups data by millions of slots
        # This helps in managing large amounts of data efficiently
        directory = Path(base_path) / data_type / f"slot_{slot // 1_000_000:07d}xxx"
        try:
            directory.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise IOError(f"Failed to create directory {directory}: {e}")
        
        # Create a standardized file name format for easy identification and sorting
        file_name = f"{data_type}_slot_{slot:010d}.parquet"
        file_path = directory / file_name
        
        # Write the DataFrame to Parquet format, which is efficient for columnar data
        df.write_parquet(file_path)
        logger.info(f"Successfully wrote {data_type} data for slot {slot} to {file_path}")
        
        return str(file_path)
    except Exception as e:
        logger.error(f"Error in write_df_to_parquet: {e}")
        raise

def find_last_processed_block(base_path: str = "data") -> Optional[int]:
    """
    Find the last processed block by examining the existing Parquet files.
    
    This function is crucial for determining where to resume processing in case of interruptions
    or when running incremental updates to the indexed data.
    
    :param base_path: str, the base directory where Parquet files are stored
    :return: Optional[int], the slot number of the last processed block, or None if no blocks have been processed
    """
    try:
        base_path = Path(base_path)
        if not base_path.exists():
            logger.info(f"Base path {base_path} does not exist")
            return None  # No data directory exists, so no blocks have been processed

        last_slot = None
        # Iterate through all expected data types to ensure we find the latest across all categories
        for data_type in ["blocks", "transactions", "instructions", "rewards"]:
            type_path = base_path / data_type
            if not type_path.exists():
                continue  # Skip if this data type directory doesn't exist

            try:
                # Check all subdirectories (million-slot ranges) for this data type
                # This approach allows for efficient searching even with large amounts of data
                for subdir in type_path.iterdir():
                    if not subdir.is_dir():
                        continue  # Skip if not a directory

                    # Look for Parquet files in this subdirectory
                    for file in subdir.glob("*_slot_*.parquet"):
                        try:
                            # Extract the slot number from the filename
                            # Assumes a consistent naming convention as used in write_df_to_parquet
                            slot = int(file.stem.split("_slot_")[1])
                            # Update last_slot if this is the highest we've seen
                            # This ensures we find the absolute latest processed slot across all data types
                            if last_slot is None or slot > last_slot:
                                last_slot = slot
                        except (IndexError, ValueError) as e:
                            logger.warning(f"Skipping file {file}: Unable to extract slot number: {e}")

            except OSError as e:
                logger.error(f"Error accessing directory {type_path}: {e}")

        if last_slot is not None:
            logger.info(f"Last processed block found: slot {last_slot}")
        else:
            logger.info("No processed blocks found")
        
        return last_slot
    except Exception as e:
        logger.error(f"Error in find_last_processed_block: {e}")
        raise