import polars as pl

class SolanaSchemas:
    """
    A class containing static methods that define schemas for various Solana blockchain data structures.
    These schemas are used for data validation and type checking when processing Solana blockchain data.
    """

    @staticmethod
    def block_schema():
        """
        Defines the schema for a Solana block.
        
        :return: dict, a dictionary mapping field names to polars data types
        """
        return {
            "parent_slot": pl.Int64, # The slot number of the parent block
            "slot": pl.Int64, # The current block's slot number
            "block_time": pl.Int64, # Unix timestamp of the block
            "block_height": pl.Int64, # Height of the block in the blockchain
            "previous_blockhash": pl.Utf8, # Hash of the previous block
            "blockhash": pl.Utf8, # Hash of the current block
        }

    @staticmethod
    def transactions_schema():
        """
        Defines the schema for Solana transactions.
        
        :return: dict, a dictionary mapping field names to polars data types
        """
        return {
            "slot": pl.Int64, # Slot number where the transaction was processed
            "signature": pl.Utf8, # Unique signature of the transaction
            "num_required_signatures": pl.Int64, # Number of required signatures
            "num_readonly_signed_accounts": pl.Int64, # Number of read-only signed accounts
            "num_readonly_unsigned_accounts": pl.Int64, # Number of read-only unsigned accounts
            "recent_blockhash": pl.Utf8, # Recent blockhash used for this transaction
            "success": pl.Boolean, # Whether the transaction was successful
            "error": pl.Utf8, # Error message if the transaction failed
            "fee": pl.Int64, # Transaction fee in lamports
            "pre_balances": pl.Utf8, # Account balances before the transaction (JSON string)
            "post_balances": pl.Utf8, # Account balances after the transaction (JSON string)
            "pre_token_balances": pl.Utf8, # Token balances before the transaction (JSON string)
            "post_token_balances": pl.Utf8, # Token balances after the transaction (JSON string)
            "log_messages": pl.Utf8, # Log messages generated during transaction execution (JSON string)
            "rewards": pl.Utf8, # Rewards associated with the transaction (JSON string)
            "compute_units_consumed": pl.Int64, # Compute units consumed by the transaction
        }

    @staticmethod
    def instructions_schema():
        """
        Defines the schema for Solana transaction instructions.
        
        :return: dict, a dictionary mapping field names to polars data types
        """
        return {
            "slot": pl.Int64, # Slot number where the instruction was processed
            "tx_signature": pl.Utf8, # Signature of the transaction containing this instruction
            "instruction_index": pl.Int64, # Index of this instruction within the transaction
            "program_id_index": pl.Int64, # Index of the program ID in the transaction's account keys
            "program_id": pl.Utf8, # The program ID (address) that processes this instruction
            "accounts": pl.Utf8, # List of accounts involved in this instruction (JSON string)
            "data": pl.Utf8, # Instruction data as a base58 encoded string
            "is_inner": pl.Boolean, # Whether this is an inner instruction
            "parent_index": pl.Int64, # Index of the parent instruction (for inner instructions)
        }

    @staticmethod
    def rewards_schema():
        """
        Defines the schema for Solana rewards.
        
        :return: dict, a dictionary mapping field names to polars data types
        """
        return {
            "slot": pl.Int64, # Slot number where the reward was given
            "pubkey": pl.Utf8, # Public key of the account receiving the reward
            "lamports": pl.Int64, # Amount of reward in lamports (can be negative)
            "post_balance": pl.Int64, # Account balance after applying the reward
            "reward_type": pl.Utf8, # Type of reward (e.g., "Fee", "Rent", "Staking", etc.)
            "commission": pl.Int64, # Commission applied to the reward (for validator rewards)
        }

    # Commented out schema for token balances
    # This schema is currently not in use but may be implemented in the future
    # token_balances_schema = {
    #     "account_index": pl.Int64, # Index of the account in the transaction
    #     "mint": pl.Utf8, # Address of the token's mint
    #     "amount": pl.Utf8, # Token amount (as string due to potential large values)
    #     "decimals": pl.Int64, # Number of decimal places for the token
    #     "ui_amount": pl.Float64, # Human-readable token amount
    #     "ui_amount_string": pl.Utf8, # Human-readable token amount as a string
    #     "owner": pl.Utf8, # Owner of the token account
    #     "program_id": pl.Utf8, # Program ID of the token program
    # }
