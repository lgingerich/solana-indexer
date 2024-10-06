import os
from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, List, Union, Any

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.table import Table
from pyiceberg.transforms import TruncateTransform, BucketTransform
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from utils import logger


# Define table schemas
table_schemas = {
    "blocks": pa.schema([
        pa.field("slot", pa.int64(), nullable=False),
        pa.field("parent_slot", pa.int64(), nullable=False),
        pa.field("block_time", pa.int64(), nullable=True),
        pa.field("block_height", pa.int64(), nullable=False),
        pa.field("previous_blockhash", pa.string(), nullable=False),
        pa.field("blockhash", pa.string(), nullable=False),
    ]),
    "transactions": pa.schema([
        pa.field("slot", pa.int64(), nullable=False),
        pa.field("signature", pa.string(), nullable=False),
        pa.field("num_required_signatures", pa.int64(), nullable=True),
        pa.field("num_readonly_signed_accounts", pa.int64(), nullable=True),
        pa.field("num_readonly_unsigned_accounts", pa.int64(), nullable=True),
        pa.field("recent_blockhash", pa.string(), nullable=True),
        pa.field("success", pa.bool_(), nullable=True),
        pa.field("error", pa.string(), nullable=True),
        pa.field("fee", pa.int64(), nullable=True),
        pa.field("pre_balances", pa.string(), nullable=True),
        pa.field("post_balances", pa.string(), nullable=True),
        pa.field("pre_token_balances", pa.string(), nullable=True),
        pa.field("post_token_balances", pa.string(), nullable=True),
        pa.field("log_messages", pa.string(), nullable=True),
        pa.field("rewards", pa.string(), nullable=True),
        pa.field("compute_units_consumed", pa.int64(), nullable=True),
    ]),
    "instructions": pa.schema([
        pa.field("slot", pa.int64(), nullable=False),
        pa.field("tx_signature", pa.string(), nullable=False),
        pa.field("instruction_index", pa.int64(), nullable=True),
        pa.field("program_id_index", pa.int64(), nullable=True),
        pa.field("program_id", pa.string(), nullable=True),
        pa.field("accounts", pa.string(), nullable=True),
        pa.field("data", pa.string(), nullable=True),
        pa.field("is_inner", pa.bool_(), nullable=True),
        pa.field("parent_index", pa.int64(), nullable=True),
    ]),
    "rewards": pa.schema([
        pa.field("slot", pa.int64(), nullable=False),
        pa.field("pubkey", pa.string(), nullable=True),
        pa.field("lamports", pa.int64(), nullable=True),
        pa.field("post_balance", pa.int64(), nullable=True),
        pa.field("reward_type", pa.string(), nullable=True),
        pa.field("commission", pa.int64(), nullable=True),
    ]),
}

# ABC is used to define an interface for different storage backends.
# It ensures implementing classes provide write_table and find_last_processed_block 
# methods, allowing for interchangeable storage solutions in the application.
class DataStore(ABC):
    @abstractmethod
    def write_table(self, data: List[Dict[str, Any]], data_type: str, slot: int) -> str:
        """Write data to storage."""
        pass

    @abstractmethod
    def find_last_processed_block(self) -> Optional[int]:
        """Find the last processed block."""
        pass

class ParquetDataStore(DataStore):
    def __init__(self, base_path: str = "data"):
        self.base_path = Path(base_path)

    def write_table(self, data: List[Dict[str, Any]], table_var: str, slot: int) -> str:
        try:
            if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
                raise ValueError(f"data must be a list of dictionaries for table {table_var}")
            if not isinstance(table_var, str) or not table_var:
                raise ValueError("table_var must be a non-empty string")
            if not isinstance(slot, int) or slot < 0:
                raise ValueError("slot must be a non-negative integer")
            
            # Calculate the correct directory name
            dir_base = (slot // 1_000_000) * 1_000_000
            directory = self.base_path / table_var / f"slot_{dir_base:09d}"
            try:
                directory.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                raise IOError(f"Failed to create directory {directory}: {e}")
            
            file_name = f"{table_var}_slot_{slot}.parquet"
            file_path = directory / file_name
            
            # Convert to a PyArrow Table. Enforce predefined table schema.
            pa_table = pa.Table.from_pylist(data, schema=table_schemas[table_var])
            
            # Write the PyArrow Table to a Parquet file
            pq.write_table(pa_table, file_path)
            logger.info(f"Successfully wrote {table_var} data for slot {slot} to {file_path}")
            
            return str(file_path)
        except Exception as e:
            logger.error(f"Error in write_table (Iceberg) for table {table_var}, slot {slot}: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Error details: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
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

class IcebergDataStore(DataStore):
    def __init__(self, warehouse_path: str = "iceberg_warehouse", catalog_name: str = "solana_indexer", namespace: str = "blockchain_data", uri: str = None):
        self.warehouse_path = Path(warehouse_path)
        self.catalog_name = catalog_name
        self.namespace = namespace
        
        # Ensure the warehouse directory exists
        os.makedirs(self.warehouse_path, exist_ok=True)
        
        if uri is None:
            # Use an absolute path for the SQLite database file
            db_path = os.path.abspath(os.path.join(self.warehouse_path, "iceberg.db"))
            uri = f"sqlite:///{db_path}"
        
        # Create the SQLite database file if it doesn't exist
        if uri.startswith("sqlite:///"):
            db_file = uri.replace("sqlite:///", "")
            if not os.path.exists(db_file):
                open(db_file, 'a').close()  # Create an empty file
        logger.warning(f"Using URI: {uri}")
        logger.warning(f"Database: {db_file}")
        logger.warning(f"Namespace: {namespace}")
        self.engine: Engine = create_engine(uri, echo=False)
        
        catalog_kwargs = {
            "warehouse": str(self.warehouse_path),
            "uri": uri,
        }
        
        self.catalog = load_catalog(
            "sqlite",
            **catalog_kwargs
        )
        
        # Create the namespace if it doesn't exist
        if not self.catalog._namespace_exists(self.namespace):
            self.catalog.create_namespace(self.namespace)
        
        self._create_tables()

    def _create_tables(self):

        # TO DO: Add partitioning to the tables
        for table_name, schema in table_schemas.items():
            if not self.catalog.table_exists(f"{self.namespace}.{table_name}"):
                self.catalog.create_table(
                    identifier=f"{self.namespace}.{table_name}",
                    schema=schema,
                    # partition_spec=partition_spec
                )

    def write_table(self, data: List[Dict[str, Any]], table_var: str, slot: int) -> str:
        try:
            if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
                raise ValueError(f"data must be a list of dictionaries for table {table_var}")
            if not isinstance(table_var, str) or not table_var:
                raise ValueError("table_var must be a non-empty string")
            if not isinstance(slot, int) or slot < 0:
                raise ValueError("slot must be a non-negative integer")
            
            logger.info(f"Writing {table_var} data for slot {slot}")
            table_name = f"{self.namespace}.{table_var}"
            table = self.catalog.load_table(table_name)
            
            # Convert to a PyArrow Table. Enforce predefined table schema.
            pa_table = pa.Table.from_pylist(data, schema=table_schemas[table_var])
            
            # Write data to Iceberg table
            with self.engine.connect() as connection:
                table.append(pa_table)

            logger.info(f"Successfully wrote {table_var} data for slot {slot} to Iceberg table: {table_name}")
            
            return f"iceberg://{self.catalog_name}/{table_name}"
        except Exception as e:
            logger.error(f"Error in write_table (Iceberg) for table {table_var}, slot {slot}: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Error details: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    def find_last_processed_block(self) -> Optional[int]:
        try:
            last_slot = None
            tables_exist = False

            for data_type in ["blocks", "transactions", "instructions", "rewards"]:
                table_name = f"{self.namespace}.{data_type}"
                try:
                    table = self.catalog.load_table(table_name)
                    tables_exist = True
                except NoSuchTableError:
                    logger.info(f"Table {table_name} does not exist")
                    continue

                # Scan the table to find the maximum slot
                with self.engine.connect() as connection:
                    scanner = table.scan()
                    max_slot_record = scanner.select("slot").to_arrow().to_pandas()
                    
                    if not max_slot_record.empty:
                        max_slot = max_slot_record['slot'].max()
                        if pd.notna(max_slot):
                            max_slot = int(max_slot)
                            if last_slot is None or max_slot > last_slot:
                                last_slot = max_slot

            if not tables_exist:
                logger.info("No tables found in the database. The database might be empty.")
                return None

            if last_slot is not None:
                logger.info(f"Last processed block found in Iceberg: slot {last_slot}")
            else:
                logger.info("No processed blocks found in Iceberg tables")
            
            return last_slot
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