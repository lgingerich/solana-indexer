import os
from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import TruncateTransform, BucketTransform
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from utils import logger


# Define table schemas
from pyiceberg.types import (
    NestedField,
    LongType,
    StringType,
    BooleanType,
)
from pyiceberg.schema import Schema

table_schemas = {
    "blocks": Schema(
        NestedField(1, "slot", LongType(), required=True),
        NestedField(2, "parent_slot", LongType(), required=True),
        NestedField(3, "block_time", LongType(), required=False),
        NestedField(4, "block_height", LongType(), required=True),
        NestedField(5, "previous_blockhash", StringType(), required=True),
        NestedField(6, "blockhash", StringType(), required=True),
    ),
    "transactions": Schema(
        NestedField(1, "slot", LongType(), required=True),
        NestedField(2, "signature", StringType(), required=True),
        NestedField(3, "num_required_signatures", LongType(), required=False),
        NestedField(4, "num_readonly_signed_accounts", LongType(), required=False),
        NestedField(5, "num_readonly_unsigned_accounts", LongType(), required=False),
        NestedField(6, "recent_blockhash", StringType(), required=False),
        NestedField(7, "success", BooleanType(), required=False),
        NestedField(8, "error", StringType(), required=False),
        NestedField(9, "fee", LongType(), required=False),
        NestedField(10, "pre_balances", StringType(), required=False),
        NestedField(11, "post_balances", StringType(), required=False),
        NestedField(12, "pre_token_balances", StringType(), required=False),
        NestedField(13, "post_token_balances", StringType(), required=False),
        NestedField(14, "log_messages", StringType(), required=False),
        NestedField(15, "rewards", StringType(), required=False),
        NestedField(16, "compute_units_consumed", LongType(), required=False),
    ),
    "instructions": Schema(
        NestedField(1, "slot", LongType(), required=True),
        NestedField(2, "tx_signature", StringType(), required=True),
        NestedField(3, "instruction_index", LongType(), required=False),
        NestedField(4, "program_id_index", LongType(), required=False),
        NestedField(5, "program_id", StringType(), required=False),
        NestedField(6, "accounts", StringType(), required=False),
        NestedField(7, "data", StringType(), required=False),
        NestedField(8, "is_inner", BooleanType(), required=False),
        NestedField(9, "parent_index", LongType(), required=False),
    ),
    "rewards": Schema(
        NestedField(1, "slot", LongType(), required=True),
        NestedField(2, "pubkey", StringType(), required=False),
        NestedField(3, "lamports", LongType(), required=False),
        NestedField(4, "post_balance", LongType(), required=False),
        NestedField(5, "reward_type", StringType(), required=False),
        NestedField(6, "commission", LongType(), required=False),
    ),
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
        logger.info(f"Using URI: {uri}")
        logger.info(f"Database: {db_file}")
        logger.info(f"Namespace: {namespace}")
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

    # TO DO: Fix partitioning
    def _create_tables(self):
        for table_name, schema in table_schemas.items():
            
            if not self.catalog.table_exists(f"{self.namespace}.{table_name}"):

                # # Create a partition spec that groups by every 1000 slots
                # partition_spec = PartitionSpec(
                #     PartitionField(
                #         source_id=1,  # Assuming 'slot' is the first field in the schema
                #         # # source_id=schema.index_of_field("slot"),
                #         field_id=1000,
                #         # # transform=TruncateTransform(width=3),
                #         # transform=TruncateTransform(width=3),
                #         # name="slot"
                #         # source_id=iceberg_schema.find_field("slot").field_id,
                #         transform=TruncateTransform(1000000),
                #         name="slot"
                #     )
                # )

                self.catalog.create_table(
                    identifier=f"{self.namespace}.{table_name}",
                    # schema=iceberg_schema,
                    schema=schema,
                    # partition_spec=partition_spec
                )
                # logger.info(f"Created table {table_name} with partition spec: {partition_spec}")
                logger.info(f"Created table {table_name}")

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