from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

def init_spark():
    spark_config = None
    # Initialize Spark session with Iceberg configurations
    builder = SparkSession.builder \
        .appName("IcebergLocalDevelopment") \
        .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2') \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", f"file:///Users/lgingerich/Documents/Code/solana-indexer/iceberg_warehouse")

    # Add any additional Spark configurations
    if spark_config:
        for key, value in spark_config.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()

    return spark

# Initialize Spark session
spark = init_spark()

# Verify Spark session creation
try:
    spark.sql("SHOW DATABASES").show()
except Exception as e:
    logger.error(f"Error verifying Spark session: {e}")
    raise

# Construct the full table name
namespace = "blockchain_data"
table_name = "blocks"
full_table_name = f"local.{namespace}.{table_name}"

# Read the Iceberg table into a DataFrame
df = spark.read.format("iceberg").load(full_table_name)

# Display the first 10 rows of the DataFrame
df.sort("slot", ascending=True).show(10)

# Close the Spark session when you're done
spark.stop()