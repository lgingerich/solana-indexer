import os
from abc import ABC, abstractmethod
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
from pyiceberg.types import NestedField, LongType, StringType, BooleanType
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, BooleanType
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from schemas import SolanaSchemas
from utils import logger

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

class SparkDataStore(DataStore):
    def __init__(self, warehouse_path, catalog_name, namespace, spark_config=None):
        self.warehouse_path = warehouse_path
        self.catalog_name = catalog_name
        self.namespace = namespace

        # Initialize Spark session with Iceberg configurations
        builder = SparkSession.builder \
            .appName("IcebergLocalDevelopment") \
            .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2') \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local.type", "hadoop") \
            .config("spark.sql.catalog.local.warehouse", f"file://{os.path.abspath(warehouse_path)}") \
            .config("spark.sql.warehouse.dir", f"file://{os.path.abspath(warehouse_path)}")

        # Add any additional Spark configurations
        if spark_config:
            for key, value in spark_config.items():
                builder = builder.config(key, value)

        self.spark = builder.getOrCreate()

        # Verify Spark session creation
        try:
            self.spark.sql("SHOW DATABASES").show()
        except Exception as e:
            logger.error(f"Error verifying Spark session: {e}")
            raise

        # Create tables if they don't exist
        self._create_tables()

    def _create_tables(self):
        for table_name, schema in SolanaSchemas.spark_schemas().items():
            full_table_name = f"local.{self.namespace}.{table_name}"
            
            try:
                # Check if table exists
                if not self.spark._jsparkSession.catalog().tableExists(full_table_name):
                    # Create the table using Spark SQL
                    create_table_sql = f"""
                        CREATE TABLE IF NOT EXISTS {full_table_name} (
                            {', '.join([f"{field.name} {self._get_spark_type_string(field.dataType)}" for field in schema.fields])}
                        ) USING iceberg
                        PARTITIONED BY (slot)
                    """
                    self.spark.sql(create_table_sql)
                    logger.info(f"Created table: {full_table_name}")
                else:
                    logger.info(f"Table already exists: {full_table_name}")
            except Exception as e:
                logger.error(f"Error creating table {full_table_name}: {e}")
                raise

    def write_table(self, data: List[Dict[str, Any]], data_type: str, slot: int) -> str:
        if not data:
            logger.warning(f"No data to write for {data_type} at slot {slot}")
            return f"local.{self.namespace}.{data_type}"

        try:
            # Create DataFrame from data
            df = self.spark.createDataFrame(data, schema=SolanaSchemas.spark_schemas()[data_type])

            # Write DataFrame to Iceberg table
            table_name = f"{self.namespace}.{data_type}"
            df.writeTo(f"local.{table_name}") \
              .using("iceberg") \
              .tableProperty("write.format.default", "parquet") \
              .append()

            logger.info(f"Successfully wrote {data_type} data for slot {slot} to Iceberg table: {table_name}")
            return f"local.{table_name}"
        except Exception as e:
            logger.error(f"Error writing {data_type} data for slot {slot}: {e}")
            raise

    def find_last_processed_block(self) -> Optional[int]:
        last_slot = None
        tables_exist = False

        for data_type in SolanaSchemas.spark_schemas().keys():
            table_name = f"local.{self.namespace}.{data_type}"
            try:
                df = self.spark.table(table_name)
                max_slot = df.agg({"slot": "max"}).collect()[0][0]
                if max_slot is not None:
                    if last_slot is None or max_slot > last_slot:
                        last_slot = max_slot
                tables_exist = True
            except Exception as e:
                logger.info(f"Table {table_name} does not exist or error accessing it: {e}")
                continue

        if not tables_exist:
            logger.info("No tables found in Spark catalog. The database might be empty.")
            return None

        if last_slot is not None:
            logger.info(f"Last processed block found in Spark: slot {last_slot}")
        else:
            logger.info("No processed blocks found in Spark tables")
        
        return last_slot

    def _get_spark_type_string(self, spark_type):
        if isinstance(spark_type, LongType):
            return "BIGINT"
        elif isinstance(spark_type, StringType):
            return "STRING"
        elif isinstance(spark_type, BooleanType):
            return "BOOLEAN"
        else:
            raise ValueError(f"Unsupported Spark type: {spark_type}")


def get_data_store(store_type: str = "spark", **kwargs) -> DataStore:
    if store_type == "spark":
        return SparkDataStore(**kwargs)
    else:
        raise ValueError(f"Unsupported data store type: {store_type}")