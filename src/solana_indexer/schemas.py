from pyspark.sql.types import StructType, StructField, LongType, StringType, BooleanType

class SparkSchemas:
    """
    Defines Spark schemas for Solana blockchain data structures.
    
    This class provides static methods to access predefined schemas for various
    Solana blockchain entities. These schemas are used for data validation,
    type checking, and structuring when processing Solana blockchain data in
    Spark-based applications.
    """

    @staticmethod
    def spark_schemas():
        """
        Returns a dictionary of Spark schemas for Solana blockchain entities.
        
        :return: dict, mapping table names to Spark StructType schemas
        """
        return {
            "blocks": SparkSchemas._block_schema(),
            "transactions": SparkSchemas._transaction_schema(),
            "instructions": SparkSchemas._instruction_schema(),
            "rewards": SparkSchemas._reward_schema(),
        }

    @staticmethod
    def _block_schema():
        """
        Defines the schema for Solana block data.
        """
        return StructType([
            StructField("slot", LongType(), nullable=False),
            StructField("parent_slot", LongType(), nullable=False),
            StructField("block_time", LongType(), nullable=True),
            StructField("block_height", LongType(), nullable=False),
            StructField("previous_blockhash", StringType(), nullable=False),
            StructField("blockhash", StringType(), nullable=False),
        ])

    @staticmethod
    def _transaction_schema():
        """
        Defines the schema for Solana transaction data.
        """
        return StructType([
            StructField("slot", LongType(), nullable=False),
            StructField("signature", StringType(), nullable=False),
            StructField("num_required_signatures", LongType(), nullable=True),
            StructField("num_readonly_signed_accounts", LongType(), nullable=True),
            StructField("num_readonly_unsigned_accounts", LongType(), nullable=True),
            StructField("recent_blockhash", StringType(), nullable=True),
            StructField("success", BooleanType(), nullable=True),
            StructField("error", StringType(), nullable=True),
            StructField("fee", LongType(), nullable=True),
            StructField("pre_balances", StringType(), nullable=True),
            StructField("post_balances", StringType(), nullable=True),
            StructField("pre_token_balances", StringType(), nullable=True),
            StructField("post_token_balances", StringType(), nullable=True),
            StructField("log_messages", StringType(), nullable=True),
            StructField("rewards", StringType(), nullable=True),
            StructField("compute_units_consumed", LongType(), nullable=True),
        ])

    @staticmethod
    def _instruction_schema():
        """
        Defines the schema for Solana instruction data.
        """
        return StructType([
            StructField("slot", LongType(), nullable=False),
            StructField("tx_signature", StringType(), nullable=False),
            StructField("instruction_index", LongType(), nullable=True),
            StructField("program_id_index", LongType(), nullable=True),
            StructField("program_id", StringType(), nullable=True),
            StructField("accounts", StringType(), nullable=True),
            StructField("data", StringType(), nullable=True),
            StructField("is_inner", BooleanType(), nullable=True),
            StructField("parent_index", LongType(), nullable=True),
        ])

    @staticmethod
    def _reward_schema():
        """
        Defines the schema for Solana reward data.
        """
        return StructType([
            StructField("slot", LongType(), nullable=False),
            StructField("pubkey", StringType(), nullable=True),
            StructField("lamports", LongType(), nullable=True),
            StructField("post_balance", LongType(), nullable=True),
            StructField("reward_type", StringType(), nullable=True),
            StructField("commission", LongType(), nullable=True),
        ])
