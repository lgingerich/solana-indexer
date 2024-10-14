import os
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, BooleanType

from schemas import SparkSchemas
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
    def __init__(self, warehouse_path, catalog_name, namespace, tables_to_save, spark_config=None):
        self.warehouse_path = warehouse_path
        self.catalog_name = catalog_name
        self.namespace = namespace
        self.tables_to_save = tables_to_save

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
        for table_name, schema in SparkSchemas.spark_schemas().items():
            if table_name not in self.tables_to_save:
                continue  # Skip table creation if it's not in tables_to_save

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
        if data_type not in self.tables_to_save:
            return f"local.{self.namespace}.{data_type}"  # Return table name without writing

        try:
            # Create DataFrame from data
            df = self.spark.createDataFrame(data, schema=SparkSchemas.spark_schemas()[data_type])

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
        max_common_slot = None
        tables_exist = False

        for data_type in self.tables_to_save:
            table_name = f"local.{self.namespace}.{data_type}"
            logger.warning(f"Checking table: {table_name}")
            try:
                df = self.spark.table(table_name)
                max_slot = df.agg({"slot": "max"}).collect()[0][0]
                logger.warning(f"Max slot: {max_slot}")

                if max_slot is not None:
                    if max_common_slot is None or max_slot < max_common_slot:
                        max_common_slot = max_slot
                    tables_exist = True
                else:
                    # If any table has no data, we can't guarantee consistency
                    logger.info(f"Table {table_name} is empty")
                    return None
            except Exception as e:
                logger.info(f"Table {table_name} does not exist or error accessing it: {e}")
                return None  # If any table is missing, we can't guarantee consistency

        if not tables_exist:
            logger.info("No tables found in Spark catalog. The database might be empty.")
            return None

        if max_common_slot is not None:
            logger.info(f"Highest common processed slot found in Spark: slot {max_common_slot}")
        else:
            logger.info("No processed slots found in Spark tables")
        
        return max_common_slot

    def _get_spark_type_string(self, spark_type):
        if isinstance(spark_type, LongType):
            return "BIGINT"
        elif isinstance(spark_type, StringType):
            return "STRING"
        elif isinstance(spark_type, BooleanType):
            return "BOOLEAN"
        else:
            raise ValueError(f"Unsupported Spark type: {spark_type}")


def get_data_store(store_type: str = "spark", tables_to_save=None, **kwargs) -> DataStore:
    if store_type == "spark":
        return SparkDataStore(tables_to_save=tables_to_save, **kwargs)
    else:
        raise ValueError(f"Unsupported data store type: {store_type}")
