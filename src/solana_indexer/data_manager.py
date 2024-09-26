from abc import ABC, abstractmethod
import polars as pl
from typing import Optional
import pandas as pd
from pathlib import Path
from utils import logger
import math
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, LongType, StringType, BooleanType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import TruncateTransform, BucketTransform
from pyiceberg.table import Table
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import os
from pyarrow import schema as pa_schema
from pyarrow import field as pa_field
import pyarrow as pa

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
        table_schemas = {
            "blocks": Schema(
                NestedField(1, "slot", LongType(), required=True),
                NestedField(2, "parent_slot", LongType(), required=True),
                NestedField(3, "block_time", LongType(), required=True),
                NestedField(4, "block_height", LongType(), required=True),
                NestedField(5, "previous_blockhash", StringType(), required=True),
                NestedField(6, "blockhash", StringType(), required=True),
            ),
            "transactions": Schema(
                NestedField(1, "slot", LongType(), required=True),
                NestedField(2, "signature", StringType(), required=True),
                NestedField(3, "num_required_signatures", LongType(), required=True),
                NestedField(4, "num_readonly_signed_accounts", LongType(), required=True),
                NestedField(5, "num_readonly_unsigned_accounts", LongType(), required=True),
                NestedField(6, "recent_blockhash", StringType(), required=True),
                NestedField(7, "success", BooleanType(), required=True),
                NestedField(8, "error", StringType()),
                NestedField(9, "fee", LongType(), required=True),
                NestedField(10, "pre_balances", StringType(), required=True),  # JSON string
                NestedField(11, "post_balances", StringType(), required=True),  # JSON string
                NestedField(12, "pre_token_balances", StringType()),  # JSON string
                NestedField(13, "post_token_balances", StringType()),  # JSON string
                NestedField(14, "log_messages", StringType()),  # JSON string
                NestedField(15, "rewards", StringType()),  # JSON string
                NestedField(16, "compute_units_consumed", LongType(), required=True),
            ),
            "instructions": Schema(
                NestedField(1, "slot", LongType(), required=True),
                NestedField(2, "tx_signature", StringType(), required=True),
                NestedField(3, "instruction_index", LongType(), required=True),
                NestedField(4, "program_id_index", LongType(), required=True),
                NestedField(5, "program_id", StringType(), required=True),
                NestedField(6, "accounts", StringType(), required=True),  # JSON string
                NestedField(7, "data", StringType(), required=True),
                NestedField(8, "is_inner", BooleanType(), required=True),
                NestedField(9, "parent_index", LongType()),
            ),
            "rewards": Schema(
                NestedField(1, "slot", LongType(), required=True),
                NestedField(2, "pubkey", StringType(), required=True),
                NestedField(3, "lamports", LongType(), required=True),
                NestedField(4, "post_balance", LongType(), required=True),
                NestedField(5, "reward_type", StringType(), required=True),
                NestedField(6, "commission", LongType()),
            ),
        }

        for table_name, schema in table_schemas.items():
            if not self.catalog.table_exists(f"{self.namespace}.{table_name}"):
                # partition_spec = PartitionSpec(
                #     PartitionField(
                #         source_id=1, field_id=1000, transform=TruncateTransform(1000000), name="slot_partition"
                #     )
                # )
                partition_spec = PartitionSpec(
                    PartitionField(
                        source_id=1, 
                        # source_id=schema.index_of_field("slot")
                        field_id=1000, 
                        # transform=BucketTransform(num_buckets=1000), 
                        transform=TruncateTransform(width=1000000),
                        name="slot_bucket"
                    )
                )
                self.catalog.create_table(
                    identifier=f"{self.namespace}.{table_name}",
                    schema=schema,
                    # partition_spec=partition_spec
                )

    def _iceberg_to_arrow_type(self, iceberg_type):
        if isinstance(iceberg_type, LongType):
            return pa.int64()
        elif isinstance(iceberg_type, StringType):
            return pa.string()
        elif isinstance(iceberg_type, BooleanType):
            return pa.bool_()
        else:
            raise ValueError(f"Unsupported Iceberg type: {iceberg_type}")


    def write_df(self, df: pl.DataFrame, data_type: str, slot: int) -> str:
        try:
            if not isinstance(df, pl.DataFrame):
                raise ValueError("df must be a Polars DataFrame")
            if not isinstance(data_type, str) or not data_type:
                raise ValueError("data_type must be a non-empty string")
            if not isinstance(slot, int) or slot < 0:
                raise ValueError("slot must be a non-negative integer")
            logger.warning(f"Writing {data_type} data for slot {slot}")
            table_name = f"{self.namespace}.{data_type}"
            table = self.catalog.load_table(table_name)
            
            # Convert Polars DataFrame to PyArrow Table
            pa_table = df.to_arrow()
            
            # Get the schema from the Iceberg table
            iceberg_schema = table.schema()
            
            # Create a new PyArrow schema that matches the Iceberg schema
            new_pa_schema = pa_schema([
                pa_field(
                    field.name, 
                    self._iceberg_to_arrow_type(field.field_type),
                    nullable=field.optional
                )
                for field in iceberg_schema.fields
                if isinstance(field, NestedField)
            ])
            
            # Cast the PyArrow table to the new schema
            pa_table = pa_table.cast(new_pa_schema)
            
            # Write data to Iceberg table
            with self.engine.connect() as connection:
                table.append(pa_table)

            logger.info(f"Successfully wrote {data_type} data for slot {slot} to Iceberg table: {table_name}")
            
            return f"iceberg://{self.catalog_name}/{table_name}"
        except Exception as e:
            logger.error(f"Error in write_df (Iceberg): {e}")
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