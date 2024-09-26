from abc import ABC, abstractmethod
import polars as pl
from typing import Optional
from pathlib import Path
from utils import logger
import math

# ABC is used to define an interface for different storage backends.
# It ensures implementing classes provide write_df and find_last_processed_block 
# methods, allowing for interchangeable storage solutions in the application.
class DataStore(ABC):
    @abstractmethod
    def write_df(self, df: pl.DataFrame, data_type: str, slot: int) -> str:
        """Write a DataFrame to storage."""
        pass

    @abstractmethod
    def find_last_processed_block(self) -> Optional[int]:
        """Find the last processed block."""
        pass

class ParquetDataStore(DataStore):
    def __init__(self, base_path: str = "data"):
        self.base_path = Path(base_path)

    def write_df(self, df: pl.DataFrame, data_type: str, slot: int) -> str:
        try:
            if not isinstance(df, pl.DataFrame):
                raise ValueError("df must be a Polars DataFrame")
            if not isinstance(data_type, str) or not data_type:
                raise ValueError("data_type must be a non-empty string")
            if not isinstance(slot, int) or slot < 0:
                raise ValueError("slot must be a non-negative integer")
            
            # Calculate the correct directory name
            dir_base = (slot // 1_000_000) * 1_000_000
            directory = self.base_path / data_type / f"slot_{dir_base:09d}"
            try:
                directory.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                raise IOError(f"Failed to create directory {directory}: {e}")
            
            file_name = f"{data_type}_slot_{slot}.parquet"
            file_path = directory / file_name
            
            df.write_parquet(file_path)
            logger.info(f"Successfully wrote {data_type} data for slot {slot} to {file_path}")
            
            return str(file_path)
        except Exception as e:
            logger.error(f"Error in write_df: {e}")
            raise

    def find_last_processed_block(self) -> Optional[int]:
        try:
            if not self.base_path.exists():
                logger.info(f"Base path {self.base_path} does not exist")
                return None

            last_slot = None
            for data_type in ["blocks", "transactions", "instructions", "rewards"]:
                type_path = self.base_path / data_type
                if not type_path.exists():
                    continue

                try:
                    for subdir in type_path.iterdir():
                        if not subdir.is_dir():
                            continue

                        for file in subdir.glob("*_slot_*.parquet"):
                            try:
                                slot = int(file.stem.split("_slot_")[1])
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

# Mock implementation for Iceberg Data Store
class IcebergDataStore(DataStore):
    def __init__(self, warehouse_path: str = "iceberg_warehouse"):
        self.warehouse_path = Path(warehouse_path)
        # In a real implementation, you would initialize Iceberg catalog and necessary configurations here
        logger.info(f"Initialized Iceberg Data Store with warehouse path: {self.warehouse_path}")

    def write_df(self, df: pl.DataFrame, data_type: str, slot: int) -> str:
        try:
            if not isinstance(df, pl.DataFrame):
                raise ValueError("df must be a Polars DataFrame")
            if not isinstance(data_type, str) or not data_type:
                raise ValueError("data_type must be a non-empty string")
            if not isinstance(slot, int) or slot < 0:
                raise ValueError("slot must be a non-negative integer")
            
            # In a real implementation, you would:
            # 1. Convert Polars DataFrame to a format compatible with Iceberg (e.g., PySpark DataFrame)
            # 2. Use Iceberg API to write the data
            # 3. Handle partitioning based on data_type and slot
            
            table_name = f"{data_type}_table"
            mock_path = self.warehouse_path / table_name / f"slot_{slot}"
            
            logger.info(f"Mock: Writing {data_type} data for slot {slot} to Iceberg table: {table_name}")
            
            return str(mock_path)
        except Exception as e:
            logger.error(f"Error in write_df (Iceberg): {e}")
            raise

    def find_last_processed_block(self) -> Optional[int]:
        try:
            # In a real implementation, you would:
            # 1. Use Iceberg API to query metadata or scan the latest partition
            # 2. Determine the last processed slot across all data types
            
            mock_last_slot = 1000000  # This is just a placeholder
            logger.info(f"Mock: Last processed block found in Iceberg: slot {mock_last_slot}")
            
            return mock_last_slot
        except Exception as e:
            logger.error(f"Error in find_last_processed_block (Iceberg): {e}")
            raise

def get_data_store(store_type: str = "parquet", **kwargs) -> DataStore:
    if store_type == "parquet":
        return ParquetDataStore(**kwargs)
    elif store_type == "iceberg":
        return IcebergDataStore(**kwargs)
    else:
        raise ValueError(f"Unsupported data store type: {store_type}")